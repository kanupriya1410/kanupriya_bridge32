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
    region_name="us-east-2",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

OUTBOUND_URL = "https://sqs.us-east-2.amazonaws.com/555464613342/bridge32-outbound-dev"

r = sqs.receive_message(
    QueueUrl=OUTBOUND_URL,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=5,
)

msgs = r.get("Messages", [])
logger.info("Messages found: %s", len(msgs))

for m in msgs:
    body = json.loads(m["Body"])
    logger.info("Type: %s", body.get("message_type"))
    logger.info("Body: %s", m["Body"][:300])
