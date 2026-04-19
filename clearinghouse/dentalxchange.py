import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

API_URL = os.getenv("DENTALXCHANGE_API_URL", "https://sandbox.dentalxchange.com/api")
USERNAME = os.getenv("DENTALXCHANGE_USERNAME")
PASSWORD = os.getenv("DENTALXCHANGE_PASSWORD")
SUB_ID = os.getenv("DENTALXCHANGE_SUBMITTER_ID")


def submit_claim(claim: dict) -> dict:
    payload = {
        "submitter_id": SUB_ID,
        "claim_id": claim.get("claim_id"),
        "patient": claim.get("patient"),
        "insurance": claim.get("insurance"),
        "procedures": claim.get("procedures"),
        "financials": claim.get("financials"),
        "billing_provider": claim.get("billing_provider"),
        "diagnosis_codes": claim.get("diagnosis_codes"),
    }
    logger.info("Submitting 837P to DentalXChange sandbox")
    try:
        response = requests.post(
            f"{API_URL}/claims/submit",
            auth=(USERNAME, PASSWORD),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("DentalXChange error: %s", e)
        return {"status": "error", "message": str(e)}


def request_eligibility(payload: dict) -> dict:
    logger.info("Sending 270 eligibility request to DentalXChange")
    try:
        response = requests.post(
            f"{API_URL}/eligibility/inquiry",
            auth=(USERNAME, PASSWORD),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("DentalXChange eligibility error: %s", e)
        return {"status": "error", "message": str(e)}
