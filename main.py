import logging
import os

from clearinghouse.dentalxchange import submit_claim
from clearinghouse.vyne import submit_claim as vyne_submit
from utils.sqs_consumer import delete_message, process_messages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

RDS_READY = os.getenv("RDS_HOST") not in [None, "", "localhost"]


def try_insert(func, message, label):
    try:
        func(message)
    except Exception as e:
        logger.error("RDS insert failed for %s: %s", label, e)
        raise


def handle_claim_submission(message):
    logger.info("--- CLAIM SUBMISSION ---")
    claim = message.get("claim", {})
    routing = message.get("routing", {})
    patient = claim.get("patient", {})
    financials = claim.get("financials", {})

    insurance = claim.get("insurance") or claim.get("payer", {})
    billing = claim.get("billing_provider") or claim.get("provider", {})
    clearing = routing.get("clearinghouse", "DENTALXCHANGE") if routing else "DENTALXCHANGE"

    logger.info("Claim ID:      %s", claim.get("claim_id"))
    logger.info("Patient:       %s %s", patient.get("first_name"), patient.get("last_name"))
    logger.info("Payer:         %s", insurance.get("payer_name"))
    logger.info("Provider NPI:  %s", billing.get("npi"))
    logger.info("Total charge:  %s", financials.get("total_charge"))
    logger.info("Procedures:    %s", [p.get("procedure_code") for p in claim.get("procedures", [])])
    logger.info("Clearinghouse: %s", clearing)

    from utils.rds_inserter import insert_claim

    try_insert(insert_claim, message, "claim")

    if clearing == "VYNE":
        result = vyne_submit(claim)
    else:
        result = submit_claim(claim)
    logger.info("Clearinghouse result: %s", result.get("status"))


def handle_insurance_verification_request(message):
    logger.info("--- INSURANCE VERIFICATION REQUEST ---")
    req = message.get("verification_request", {})
    routing = message.get("routing", {})
    clearing = routing.get("clearinghouse", "DENTALXCHANGE") if routing else "DENTALXCHANGE"
    patient = req.get("patient", {})
    insurance = req.get("insurance", {})

    logger.info("Request ID:    %s", req.get("request_id"))
    logger.info("Patient:       %s %s", patient.get("first_name"), patient.get("last_name"))
    logger.info("Payer:         %s", insurance.get("payer_name"))
    logger.info("Member ID:     %s", insurance.get("member_id"))
    logger.info("Appointment:   %s", req.get("appointment", {}).get("date"))
    logger.info("Clearinghouse: %s", clearing)

    from utils.rds_inserter import insert_eligibility_request

    try_insert(insert_eligibility_request, message, "eligibility_request")
    logger.info("Send 270 to %s — pending API credentials", clearing)


def handle_insurance_verification_result(message):
    logger.info("--- INSURANCE VERIFICATION RESULT ---")
    result = message.get("verification_result", {})
    coverage = result.get("coverage", {})
    benefits = result.get("benefits", [])

    logger.info("Request ID:    %s", result.get("request_id"))
    logger.info("Status:        %s", result.get("status"))
    logger.info("Payer:         %s", result.get("payer", {}).get("payer_name"))
    logger.info("Coverage:      %s", "ACTIVE" if coverage.get("active") else "INACTIVE")
    logger.info("Plan type:     %s", coverage.get("plan_type"))
    logger.info("Effective:     %s", coverage.get("effective_date"))
    logger.info("Benefits:      %s found", len(benefits))
    logger.info("Confidence:    %s", result.get("confidence_score"))

    for b in benefits:
        logger.info(
            "Benefit: %s | %s%% | Max: $%s",
            b.get("service_category"),
            b.get("coverage_percentage"),
            b.get("annual_maximum"),
        )

    from utils.rds_inserter import insert_eligibility_result

    try_insert(insert_eligibility_result, message, "eligibility_result")


def handle_era_received(message):
    logger.info("--- ERA RECEIVED ---")
    era = message.get("era", {})
    payer = era.get("payer", {})
    batches = era.get("payment_batches", [])

    logger.info("ERA ID:    %s", era.get("era_id"))
    logger.info("File:      %s", era.get("file_name"))
    logger.info("S3 bucket: %s", era.get("s3_bucket"))
    logger.info("S3 key:    %s", era.get("s3_key"))
    logger.info("Payer:     %s", payer.get("payer_name"))
    logger.info("Batches:   %s", len(batches))

    for b in batches:
        payment = b.get("payment", {})
        claims = b.get("claims", [])
        logger.info(
            "Payment: $%s via %s",
            payment.get("payment_amount"),
            payment.get("payment_method"),
        )
        for c in claims:
            fin = c.get("financials", {})
            logger.info(
                "Claim: %s | Charged: $%s | Paid: $%s",
                c.get("status"),
                fin.get("total_charge"),
                fin.get("paid_amount"),
            )

    from utils.rds_inserter import insert_era

    try_insert(insert_era, message, "era")
    logger.info("ERA handoff to Goura pyx12 parser — pending")


def main():
    logger.info("=" * 50)
    logger.info("Bridge32 — SQS to RDS to Clearinghouse")
    logger.info("=" * 50)

    for message, receipt_handle in process_messages():
        msg_type = message.get("message_type")
        logger.info("Received: %s", msg_type)
        try:
            if msg_type == "CLAIM_SUBMISSION":
                handle_claim_submission(message)
            elif msg_type == "INSURANCE_VERIFICATION_REQUEST":
                handle_insurance_verification_request(message)
            elif msg_type == "INSURANCE_VERIFICATION_RESULT":
                handle_insurance_verification_result(message)
            elif msg_type == "ERA_RECEIVED":
                handle_era_received(message)
            else:
                logger.warning("Unknown message type: %s", msg_type)

            delete_message(receipt_handle)
            logger.info("Message deleted from SQS")

        except Exception as e:
            logger.error("Error processing %s: %s", msg_type, e)
            logger.warning("Message NOT deleted — will retry")

    logger.info("Done!")


if __name__ == "__main__":
    main()
