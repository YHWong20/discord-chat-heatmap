"""
Script to trigger transcription job on Amazon Transcribe.
"""

import os
import datetime
import logging
import boto3

input_bucket_name = os.environ["inBucketName"]
output_bucket_name = os.environ["outBucketName"]
today = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")


def retrieve_flac_uri_from_s3(bucket_name):
    """
    Function to retrieve URI of latest FLAC file uploaded to S3.

    Args:
        bucket_name (str): Name of input file S3 bucket

    Returns:
        uri (str): S3 URI of the latest FLAC file
    """
    s3_client = boto3.client("s3")

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
        latest_object_uri = f"s3://{bucket_name}/{latest_object_key}"
    except Exception as err:
        logging.error(
            "Unable to retrieve latest FLAC file URI. Error: %s", err
        )

    logging.info("Successfully retrieved latest FLAC file URI.")
    return latest_object_uri


def start_transcription(flac_uri, out_bucket_name):
    """
    Function to retrieve URI of latest FLAC file uploaded to S3.

    Args:
        bucket_name (str): Name of input file S3 bucket

    Returns:
        uri (str): S3 URI of the latest FLAC file
    """
    transcribe_client = boto3.client("transcribe")

    try:
        job_response = transcribe_client.start_transcription_job(
            TranscriptionJobName=f"transcribe-job-{today}",
            LanguageCode="en-US",
            MediaFormat="flac",
            Media={"MediaFileUri": flac_uri},
            OutputBucketName=out_bucket_name,
            OutputKey=f"transcribed-session-{today}.json",
        )
    except Exception as err:
        logging.error("Unable to start transcription job. Error: %s", err)

    return job_response


def lambda_handler(event, context):
    """
    Lambda Handler
    """
    latest_flac_uri = retrieve_flac_uri_from_s3(input_bucket_name)

    transcription_job_status = start_transcription(
        latest_flac_uri, output_bucket_name
    )["TranscriptionJob"]["TranscriptionJobStatus"]

    if transcription_job_status != "FAILED":
        logging.info("Transcription job started.")
        return {"200": "OK"}

    logging.error("Transcription job failed to start.")
    return {"400": "ERROR"}
