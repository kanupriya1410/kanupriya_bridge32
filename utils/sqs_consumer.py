import json
import logging
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

sqs = boto3.client(
    "sqs",
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

QUEUE_URL = os.getenv("SQS_INBOUND_QUEUE_URL")


def read_messages(max_messages: int = 10) -> list:
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=5,
    )
    messages = response.get("Messages", [])
    logger.info("Received %s messages from SQS", len(messages))
    return messages


def delete_message(receipt_handle: str) -> None:
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info("Message deleted from SQS")


def process_messages():
    messages = read_messages()
    for msg in messages:
        body = json.loads(msg["Body"])
        msg_type = body.get("message_type")
        logger.info("Message type: %s", msg_type)
        yield body, msg["ReceiptHandle"]
