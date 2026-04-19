import logging
import os
import sys
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sqs = boto3.client(
    "sqs",
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

QUEUE_URL = os.getenv("SQS_INBOUND_QUEUE_URL")


def check_queue() -> int:
    attr = sqs.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )
    visible = attr["Attributes"]["ApproximateNumberOfMessages"]
    invisible = attr["Attributes"]["ApproximateNumberOfMessagesNotVisible"]
    logger.info("Messages in queue:   %s", visible)
    logger.info("Messages processing: %s", invisible)
    logger.info("Total:               %s", int(visible) + int(invisible))
    return int(visible)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "watch":
        logger.info("Watching queue every 10 seconds... (Ctrl+C to stop)")
        while True:
            count = check_queue()
            if count > 0:
                logger.info("Run: python main.py")
            time.sleep(10)
    else:
        check_queue()
