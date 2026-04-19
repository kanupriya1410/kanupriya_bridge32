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

QUEUE_URL = os.getenv("SQS_INBOUND_QUEUE_URL")


def send(msg: dict, label: str) -> None:
    r = sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(msg))
    logger.info("Sent %s → ID: %s", label, r["MessageId"])


now = datetime.now(UTC).isoformat()

# ── Message 1: INSURANCE_VERIFICATION_REQUEST ──
send(
    {
        "message_type": "INSURANCE_VERIFICATION_REQUEST",
        "event_id": str(uuid.uuid4()),
        "timestamp": now,
        "source": {
            "system": "LSA",
            "clinic_id": str(uuid.uuid4()),
            "tenant_id": str(uuid.uuid4()),
            "pms": "OpenDental",
        },
        "routing": {
            "clearinghouse": "DENTALXCHANGE",
            "interchange_control_number": "ICN001",
        },
        "verification_request": {
            "request_id": str(uuid.uuid4()),
            "inquiry_trace_number": "TRC001",
            "service_type_codes": ["30", "35"],
            "patient": {
                "patient_id": str(uuid.uuid4()),
                "first_name": "Sarah",
                "last_name": "Connor",
                "dob": "1990-05-15",
                "gender": "F",
            },
            "insurance": {
                "payer_id": "DELTA001",
                "payer_code": "DELTA",
                "payer_name": "Delta Dental",
                "member_id": "DEL123456",
                "group_number": "GRP001",
            },
            "provider": {
                "provider_id": str(uuid.uuid4()),
                "npi": "1234567890",
                "tax_id": "123456789",
            },
            "appointment": {
                "appointment_id": str(uuid.uuid4()),
                "date": "2026-04-20",
            },
            "procedures": [
                {"procedure_code": "D1110"},
                {"procedure_code": "D0210"},
            ],
        },
    },
    "INSURANCE_VERIFICATION_REQUEST",
)

# ── Message 2: CLAIM_SUBMISSION ──
send(
    {
        "message_type": "CLAIM_SUBMISSION",
        "event_id": str(uuid.uuid4()),
        "timestamp": now,
        "source": {
            "system": "LSA",
            "pms": "OpenDental",
            "clinic_id": str(uuid.uuid4()),
            "tenant_id": str(uuid.uuid4()),
            "location_id": str(uuid.uuid4()),
        },
        "routing": {
            "clearinghouse": "DENTALXCHANGE",
            "interchange_control_number": "ICN002",
        },
        "claim": {
            "claim_id": str(uuid.uuid4()),
            "external_claim_id": "EXT001",
            "place_of_service": "11",
            "claim_frequency_code": "1",
            "prior_authorization_number": "",
            "diagnosis_codes": [{"code": "K02.9", "qualifier": "ABK"}],
            "patient": {
                "patient_id": str(uuid.uuid4()),
                "first_name": "James",
                "last_name": "Miller",
                "dob": "1975-08-22",
                "gender": "M",
            },
            "insurance": {
                "payer_id": "AETNA001",
                "payer_code": "AETNA",
                "payer_name": "Aetna",
                "member_id": "AET789012",
                "group_number": "GRP002",
            },
            "billing_provider": {
                "provider_id": str(uuid.uuid4()),
                "npi": "1234567890",
                "tax_id": "123456789",
                "taxonomy_code": "1223G0001X",
                "name": "Dr. Smith Dental",
            },
            "rendering_provider": {
                "provider_id": str(uuid.uuid4()),
                "npi": "0987654321",
                "taxonomy_code": "1223G0001X",
                "first_name": "John",
                "last_name": "Smith",
            },
            "financials": {
                "total_charge": 1200.50,
                "patient_responsibility": 190.00,
                "currency": "USD",
            },
            "procedures": [
                {
                    "service_line_reference": "1",
                    "procedure_code": "D1110",
                    "description": "Prophylaxis",
                    "charge": 200.00,
                    "tooth": "",
                    "tooth_surface": "",
                    "tooth_quadrant": "",
                    "date_of_service": "2026-04-18",
                    "diagnosis_code_pointers": ["1"],
                },
                {
                    "service_line_reference": "2",
                    "procedure_code": "D2740",
                    "description": "Crown - porcelain",
                    "charge": 1000.50,
                    "tooth": "14",
                    "tooth_surface": "",
                    "tooth_quadrant": "",
                    "date_of_service": "2026-04-18",
                    "diagnosis_code_pointers": ["1"],
                },
            ],
            "attachments": [
                {
                    "attachment_id": str(uuid.uuid4()),
                    "type": "XRAY",
                    "s3_key": "attachments/xray_001.jpg",
                    "procedure_code_reference": "D2740",
                }
            ],
            "submission": {
                "status": "PENDING",
                "clearinghouse_reference": "",
                "submitted_at": "",
                "rejection_reason_code": "",
                "rejection_description": "",
            },
            "metadata": {
                "created_at": now,
                "updated_at": now,
            },
        },
    },
    "CLAIM_SUBMISSION",
)

# ── Message 3: INSURANCE_VERIFICATION_RESULT ──
send(
    {
        "message_type": "INSURANCE_VERIFICATION_RESULT",
        "event_id": str(uuid.uuid4()),
        "timestamp": now,
        "source": {
            "clinic_id": str(uuid.uuid4()),
            "tenant_id": str(uuid.uuid4()),
        },
        "verification_result": {
            "request_id": str(uuid.uuid4()),
            "inquiry_trace_number": "TRC001",
            "status": "SUCCESS",
            "payer": {
                "payer_id": "DELTA001",
                "payer_code": "DELTA",
                "payer_name": "Delta Dental",
            },
            "patient": {
                "patient_id": str(uuid.uuid4()),
                "patient_account_number": "ACC001",
            },
            "coverage": {
                "active": True,
                "plan_type": "PPO",
                "effective_date": "2026-01-01",
                "termination_date": "2026-12-31",
            },
            "benefits": [
                {
                    "service_category": "PREVENTIVE",
                    "service_category_code": "35",
                    "procedure_code": "D1110",
                    "network_indicator": "IN_NETWORK",
                    "coverage_percentage": 100,
                    "annual_deductible": 0.00,
                    "deductible_met": 0.00,
                    "remaining_deductible": 0.00,
                    "annual_maximum": 1500.00,
                    "remaining_maximum": 1500.00,
                },
                {
                    "service_category": "MAJOR",
                    "service_category_code": "87",
                    "procedure_code": "D2740",
                    "network_indicator": "IN_NETWORK",
                    "coverage_percentage": 50,
                    "annual_deductible": 1000.00,
                    "deductible_met": 350.00,
                    "remaining_deductible": 650.00,
                    "annual_maximum": 1500.00,
                    "remaining_maximum": 800.00,
                },
            ],
            "exclusions": [
                {
                    "service_category": "ORTHODONTIA",
                    "service_category_code": "34",
                    "description": "ORTHODONTIA NOT COVERED UNDER THIS PLAN",
                }
            ],
            "limitations": [
                {
                    "type": "FREQUENCY",
                    "service_category": "PREVENTIVE",
                    "description": "PROPHYLAXIS 2 PER CALENDAR YEAR COVERED",
                }
            ],
            "notes": "Patient is eligible for services",
            "confidence_score": 0.95,
        },
    },
    "INSURANCE_VERIFICATION_RESULT",
)

# ── Message 4: ERA_RECEIVED ──
send(
    {
        "message_type": "ERA_RECEIVED",
        "event_id": str(uuid.uuid4()),
        "timestamp": now,
        "source": {
            "system": "S3",
            "clearinghouse": "DENTALXCHANGE",
            "clinic_id": str(uuid.uuid4()),
            "tenant_id": str(uuid.uuid4()),
            "pms": "OpenDental",
        },
        "era": {
            "era_id": str(uuid.uuid4()),
            "file_name": "835_20260418_001.txt",
            "s3_bucket": "bridge32-era-files-dev",
            "s3_key": "era/835_20260418_001.txt",
            "received_at": now,
            "interchange_control_number": "ICN003",
            "production_flag": "P",
            "payer": {
                "payer_id": "AETNA001",
                "payer_name": "Aetna",
                "payer_address": {
                    "street": "151 Farmington Ave",
                    "city": "Hartford",
                    "state": "CT",
                    "zip": "06156",
                },
            },
            "payee": {
                "provider_name": "Dr. Smith Dental",
                "npi": "1234567890",
                "tax_id": "123456789",
                "provider_number": "PROV001",
            },
            "payment_batches": [
                {
                    "batch_id": str(uuid.uuid4()),
                    "transaction_set_id": "TXN001",
                    "payment": {
                        "payment_method": "ACH",
                        "transaction_handling_code": "CCP",
                        "payment_amount": 12769.50,
                        "payment_date": "2026-04-18",
                        "check_issue_date": "2026-04-18",
                        "trace_number": "TRC12345",
                        "currency": "USD",
                    },
                    "claims": [
                        {
                            "claim_id": str(uuid.uuid4()),
                            "external_claim_id": "EXT001",
                            "status": "PAID",
                            "patient": {
                                "first_name": "James",
                                "last_name": "Miller",
                                "member_id": "AET789012",
                            },
                            "financials": {
                                "total_charge": 1200.50,
                                "allowed_amount": 1000.00,
                                "paid_amount": 810.00,
                                "patient_responsibility": 190.00,
                                "contractual_adjustment": 200.50,
                            },
                            "matching": {
                                "status": "UNMATCHED",
                                "confidence_score": None,
                                "matched_claim_id": None,
                            },
                        }
                    ],
                }
            ],
        },
    },
    "ERA_RECEIVED",
)

logger.info("All 4 test messages sent to SQS!")
logger.info("Now run: python main.py")
