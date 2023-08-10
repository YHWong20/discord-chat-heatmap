"""
Script to process transcription job data from Amazon Transcribe.
"""

import os
import json
import datetime
import logging
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import boto3

output_bucket_name = os.environ["outBucketName"]
site_bucket_name = os.environ["siteBucketName"]
local_wordcloud_path = os.environ["wordcloudPath"]
wordcloud_key = os.environ["wordcloudS3Key"]
index_path = os.environ["indexPath"]
index_key = os.environ["indexKey"]
sns_topic_arn = os.environ["snsArn"]
today = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

sns_client = boto3.client("sns")


def check_last_job_status():
    """
    Get latest job status.

    Returns:
        latest_job_status (str): Job status
    """
    transcribe_client = boto3.client("transcribe")

    try:
        jobs = transcribe_client.list_transcription_jobs()[
            "TranscriptionJobSummaries"
        ]
        latest_job_status = sorted(
            jobs, key=lambda x: x["CompletionTime"], reverse=True
        )[0]["TranscriptionJobStatus"]

        return latest_job_status
    except Exception as err:
        logging.error(
            "Unable to check transcription job status. Error: %s", err
        )
        return "FAILED"


def retrieve_transcript_from_s3(bucket_name):
    """
    Function to retrieve latest transcript uploaded to S3.

    Args:
        bucket_name (str): Name of output file S3 bucket

    Returns:
        full_transcript (str): Entire text transcript
    """
    s3_client = boto3.client("s3")
    full_transcript = ""

    try:
        bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    except Exception as err:
        logging.error("Unable to list objects. Error: %s", err)

    try:
        # Get latest object
        latest_object_key = sorted(
            bucket_objects["Contents"],
            key=lambda x: x["LastModified"],
            reverse=True,
        )[0]["Key"]

        latest_transcript_object = s3_client.get_object(
            Bucket=bucket_name, Key=latest_object_key
        )

        # Read JSON response
        # Return a list of dicts representing transcription output
        transcript_response = json.load(latest_transcript_object["Body"])
        transcript_blocks = transcript_response["results"]["transcripts"]

        for block in transcript_blocks:
            # Append paragraphs into a full transcript
            full_transcript += block.get("transcript")
    except Exception as err:
        logging.error("Unable to retrieve full transcript. Error: %s", err)

    return full_transcript


def generate_and_upload_wordcloud(transcript):
    """
    Generate wordcloud from transcript.

    Args:
        transcript (str): Full text transcript to be generated into wordcloud
    """
    try:
        # Generate wordcloud image
        wordcloud = WordCloud(
            width=500, height=300, max_font_size=50
        ).generate(transcript)

        plt.figure()
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")

        # Save a copy of wordcloud image locally
        plt.savefig(local_wordcloud_path)
    except Exception as err:
        logging.error(
            "Unable to generate wordcloud from transcript. Error: %s", err
        )

    try:
        # Upload wordcloud image to site bucket
        s3_resource = boto3.resource("s3")
        s3_resource.Bucket(site_bucket_name).upload_file(
            local_wordcloud_path, wordcloud_key
        )
    except Exception as err:
        logging.error("Unable to upload wordcloud image to S3. Error: %s", err)


def generate_word_analytics(transcript):
    """
    Generate analytics for transcript.

    Args:
        transcript (str): Transcript
    """
    result = {}

    # Process text using wordcloud method (remove stopwords)
    words = WordCloud().process_text(transcript)
    result["Words By Count"] = words

    # Get sum of words in processed text
    result["Word Count"] = 0
    for word_pair in words.items():
        result["Word Count"] += word_pair[1]

    return result


def generate_site_index(transcript, bucket_name, wc_key, idx_key):
    """
    Generate static wordcloud site.

    Args:
        bucket_name (str): Static site bucket name
        wc_key (str): WordCloud image S3 key
        idx_key (str): HTML index S3 key
    """
    # URL of wordcloud png file
    wordcloud_url = f"https://{bucket_name}.s3.amazonaws.com/{wc_key}"

    # Get html template from s3
    s3_client = boto3.client("s3")
    template_obj = s3_client.get_object(Bucket=bucket_name, Key=idx_key)

    # Get analytics for transcript
    analytics = generate_word_analytics(transcript)
    total_word_count = analytics["Word Count"]
    top_3_words = sorted(
        analytics["Words By Count"].items(), key=lambda x: x[1], reverse=True
    )[:3]

    # Decode html template object into a string
    template = template_obj["Body"].read().decode("utf-8")

    # Update template with data and references
    updated_template = (
        template.replace("{{image_link}}", wordcloud_url)
        .replace(
            "{{date}}", datetime.datetime.now().strftime("%d %b %Y, %I:%M%p")
        )
        .replace("{{wordcount}}", str(total_word_count))
        .replace("{{word1}}", top_3_words[0][0])
        .replace("{{word2}}", top_3_words[1][0])
        .replace("{{word3}}", top_3_words[2][0])
        .replace("{{count1}}", str(top_3_words[0][1]))
        .replace("{{count2}}", str(top_3_words[1][1]))
        .replace("{{count3}}", str(top_3_words[2][1]))
    )

    # Generate new template and upload to S3
    with open(index_path, "w", encoding="utf-8") as index_file:
        index_file.write(updated_template)
    s3_resource = boto3.resource("s3")
    s3_resource.Bucket(site_bucket_name).upload_file(
        index_path, "index.html", ExtraArgs={"ContentType": "text/html"}
    )


def lambda_handler(event, context):
    """
    Lambda Handler
    """
    latest_job_status = check_last_job_status()

    if latest_job_status != "FAILED":
        # Process transcript provided that job didn't fail
        full_transcript = retrieve_transcript_from_s3(output_bucket_name)
        generate_and_upload_wordcloud(full_transcript)
        generate_site_index(
            full_transcript, site_bucket_name, wordcloud_key, index_key
        )

    # Notification
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message="""
        Site link:
        http://{0}.s3-website-us-east-1.amazonaws.com/
        """.format(
            site_bucket_name
        ),
        Subject=f"Transcription Job - {latest_job_status}",
    )

    return {200: "OK"}
