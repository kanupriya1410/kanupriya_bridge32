import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

VYNE_API_URL = os.getenv("VYNE_API_URL", "https://api.onederful.co")
VYNE_API_KEY = os.getenv("VYNE_API_KEY", "")


def submit_claim(claim: dict) -> dict:
    logger.info("Vyne mock claim: %s", claim.get("claim_id"))
    return {
        "status": "mock_success",
        "claim_id": claim.get("claim_id"),
        "message": "Vyne mock — credentials pending",
    }


def retrieve_era(practice_id: str) -> list:
    logger.info("Vyne mock ERA retrieval: %s", practice_id)
    return []


def request_eligibility(payload: dict) -> dict:
    logger.info(
        "Vyne mock eligibility: %s",
        payload.get("patient", {}).get("first_name"),
    )
    return {
        "status": "mock_success",
        "message": "Vyne mock — credentials pending",
    }
