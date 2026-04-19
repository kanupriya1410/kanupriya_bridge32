import json
import logging
import os
import uuid
from datetime import UTC, datetime

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

now = datetime.now(UTC).isoformat()

msg = {
    "message_type": "CLAIM_SUBMISSION",
    "event_id": str(uuid.uuid4()),
    "timestamp": now,
    "source": {
        "system": "LSA",
        "pms": "OpenDental",
        "clinic_id": "CLINIC-001",
        "location_id": "LOC-001",
    },
    "claim": {
        "claim_id": str(uuid.uuid4()),
        "external_claim_id": "EXT-TEST-001",
        "patient": {
            "patient_id": str(uuid.uuid4()),
            "first_name": "John",
            "last_name": "Smith",
            "dob": "1979-06-23",
        },
        "provider": {
            "provider_id": "1",
            "npi": "51236994785",
        },
        "payer": {
            "payer_id": "99999",
            "payer_name": "Metlife",
        },
        "financials": {
            "total_charge": 140,
            "currency": "USD",
        },
        "procedures": [
            {
                "procedure_code": "T1356",
                "description": "Cleaning",
                "charge": 60,
                "tooth": "",
                "date_of_service": "2026-04-19",
            },
            {
                "procedure_code": "T3541",
                "description": "X-Ray",
                "charge": 70,
                "tooth": "",
                "date_of_service": "2026-04-19",
            },
            {
                "procedure_code": "T1254",
                "description": "Exam",
                "charge": 10,
                "tooth": "",
                "date_of_service": "2026-04-19",
            },
        ],
        "metadata": {
            "created_at": now,
            "updated_at": now,
        },
    },
}

r = sqs.send_message(
    QueueUrl=OUTBOUND_URL,
    MessageBody=json.dumps(msg),
)
logger.info("Test message sent to outbound queue!")
logger.info("Message ID: %s", r["MessageId"])
logger.info("Patient: John Smith | Payer: Metlife | Total: $140")
logger.info("Now run: python utils/sqs_bridge.py")
