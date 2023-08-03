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

    jobs = transcribe_client.list_transcription_jobs()[
        "TranscriptionJobSummaries"
    ]
    latest_job_status = sorted(
        jobs, key=lambda x: x["CompletionTime"], reverse=True
    )[0]["TranscriptionJobStatus"]

    return latest_job_status


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
        transcript_response = json.load(
            latest_transcript_object["Body"].read()
        )
        transcript_blocks = transcript_response["results"]["transcripts"]

        for block in transcript_blocks:
            # Append paragraphs into a full transcript
            full_transcript += block.get("transcript")
        logging.info("Full transcript retrieved.")
    except Exception as err:
        logging.error("Unable to retrieve full transcript. Error: %s", err)

    return full_transcript


def generate_wordcloud(transcript):
    """
    Generate wordcloud from transcript.

    Args:
        transcript (str): Full text transcript to be generated into wordcloud
    """
    try:
        wordcloud = WordCloud(max_font_size=50).generate(transcript)

        plt.figure()
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")

        # Save a copy of wordcloud image locally
        plt.savefig(local_wordcloud_path)
    except Exception as err:
        logging.error(
            "Unable to generate wordcloud from transcript. Error: %s", err
        )


def lambda_handler(event, context):
    """
    Lambda Handler
    """
    latest_job_status = check_last_job_status()

    if latest_job_status != "FAILED":
        full_transcript = retrieve_transcript_from_s3(output_bucket_name)
        generate_wordcloud(full_transcript)

    sns_client.publish(
        Message="Test", Subject=f"Transcription Job - {latest_job_status}"
    )
