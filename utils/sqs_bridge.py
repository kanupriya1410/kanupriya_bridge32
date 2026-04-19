import json
import logging
import os

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

OUTBOUND_URL = "https://sqs.us-east-2.amazonaws.com/555464613342/bridge32-outbound-dev"
INBOUND_URL = "https://sqs.us-east-2.amazonaws.com/555464613342/bridge32-inbound-dev"


def bridge_messages() -> int:
    logger.info("Reading from outbound queue...")

    response = sqs.receive_message(
        QueueUrl=OUTBOUND_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5,
    )

    messages = response.get("Messages", [])
    logger.info("Found %s messages in outbound queue", len(messages))

    for msg in messages:
        body = msg["Body"]
        parsed = json.loads(body)
        msg_type = parsed.get("message_type", "UNKNOWN")

        logger.info("Bridging: %s", msg_type)

        sqs.send_message(
            QueueUrl=INBOUND_URL,
            MessageBody=body,
        )
        logger.info("Copied to inbound queue")

        sqs.delete_message(
            QueueUrl=OUTBOUND_URL,
            ReceiptHandle=msg["ReceiptHandle"],
        )
        logger.info("Deleted from outbound queue")

    return len(messages)


if __name__ == "__main__":
    count = bridge_messages()
    if count > 0:
        logger.info("Bridged %s messages! Now run: python main.py", count)
    else:
        logger.info("No messages in outbound queue")
