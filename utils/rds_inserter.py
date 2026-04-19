import json
import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_rds_connection():
    return psycopg2.connect(
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT", 5432),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD"),
        database=os.getenv("RDS_DATABASE"),
    )


def insert_claim(message):
    claim = message.get("claim", {})
    source = message.get("source", {})
    routing = message.get("routing", {})
    patient = claim.get("patient", {})
    financials = claim.get("financials", {})
    submission = claim.get("submission", {})

    insurance = claim.get("insurance") or claim.get("payer", {})
    billing = claim.get("billing_provider") or claim.get("provider", {})

    conn = get_rds_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO claim (
                claim_id, external_claim_id, clinic_id, tenant_id,
                patient_id, patient_first_name, patient_last_name,
                patient_dob, patient_gender,
                payer_id, payer_name, member_id, group_number,
                billing_provider_npi, billing_provider_tax_id,
                total_charge, patient_responsibility, currency,
                place_of_service, claim_frequency_code,
                diagnosis_codes, procedures, attachments,
                clearinghouse, submission_status, source_system
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
        """,
            (
                claim.get("claim_id"),
                claim.get("external_claim_id"),
                source.get("clinic_id"),
                source.get("tenant_id"),
                patient.get("patient_id"),
                patient.get("first_name"),
                patient.get("last_name"),
                patient.get("dob") or None,
                patient.get("gender"),
                insurance.get("payer_id"),
                insurance.get("payer_name"),
                insurance.get("member_id"),
                insurance.get("group_number"),
                billing.get("npi"),
                billing.get("tax_id"),
                financials.get("total_charge"),
                financials.get("patient_responsibility"),
                financials.get("currency", "USD"),
                claim.get("place_of_service"),
                claim.get("claim_frequency_code"),
                json.dumps(claim.get("diagnosis_codes", [])),
                json.dumps(claim.get("procedures", [])),
                json.dumps(claim.get("attachments", [])),
                routing.get("clearinghouse") if routing else None,
                submission.get("status", "PENDING") if submission else "PENDING",
                source.get("system", "LSA"),
            ),
        )
        conn.commit()
        logger.info("Claim inserted: %s", claim.get("claim_id"))
    except Exception as e:
        conn.rollback()
        logger.error("Claim insert failed: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()


def insert_eligibility_request(message):
    source = message.get("source", {})
    routing = message.get("routing", {})
    req = message.get("verification_request", {})
    patient = req.get("patient", {})
    insurance = req.get("insurance", {})
    provider = req.get("provider", {})
    appt = req.get("appointment", {})

    conn = get_rds_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO eligibility_check (
                request_id, inquiry_trace_number,
                clinic_id, tenant_id,
                patient_id, patient_first_name, patient_last_name,
                patient_dob, patient_gender,
                payer_id, payer_code, payer_name,
                member_id, group_number,
                provider_npi, provider_tax_id,
                appointment_id, appointment_date,
                service_type_codes, procedures,
                clearinghouse, status, source_system
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s
            )
        """,
            (
                req.get("request_id"),
                req.get("inquiry_trace_number"),
                source.get("clinic_id"),
                source.get("tenant_id"),
                patient.get("patient_id"),
                patient.get("first_name"),
                patient.get("last_name"),
                patient.get("dob") or None,
                patient.get("gender"),
                insurance.get("payer_id"),
                insurance.get("payer_code"),
                insurance.get("payer_name"),
                insurance.get("member_id"),
                insurance.get("group_number"),
                provider.get("npi"),
                provider.get("tax_id"),
                appt.get("appointment_id"),
                appt.get("date") or None,
                json.dumps(req.get("service_type_codes", [])),
                json.dumps(req.get("procedures", [])),
                routing.get("clearinghouse") if routing else None,
                "PENDING",
                source.get("system", "LSA"),
            ),
        )
        conn.commit()
        logger.info("Eligibility request inserted: %s", req.get("request_id"))
    except Exception as e:
        conn.rollback()
        logger.error("Eligibility insert failed: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()


def insert_eligibility_result(message):
    result = message.get("verification_result", {})
    payer = result.get("payer", {})
    coverage = result.get("coverage", {})
    benefits = result.get("benefits", [])

    conn = get_rds_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE eligibility_check SET
                status           = %s,
                payer_id         = %s,
                payer_name       = %s,
                coverage_active  = %s,
                plan_type        = %s,
                effective_date   = %s,
                termination_date = %s,
                benefits         = %s,
                exclusions       = %s,
                limitations      = %s,
                notes            = %s,
                confidence_score = %s,
                updated_at       = NOW()
            WHERE request_id = %s
        """,
            (
                result.get("status"),
                payer.get("payer_id"),
                payer.get("payer_name"),
                coverage.get("active"),
                coverage.get("plan_type"),
                coverage.get("effective_date") or None,
                coverage.get("termination_date") or None,
                json.dumps(benefits),
                json.dumps(result.get("exclusions", [])),
                json.dumps(result.get("limitations", [])),
                result.get("notes"),
                result.get("confidence_score"),
                result.get("request_id"),
            ),
        )
        conn.commit()
        logger.info("Eligibility result updated: %s", result.get("request_id"))
    except Exception as e:
        conn.rollback()
        logger.error("Eligibility result update failed: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()


def insert_era(message):
    source = message.get("source", {})
    era = message.get("era", {})
    payer = era.get("payer", {})
    payee = era.get("payee", {})

    conn = get_rds_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO era (
                era_id, file_name, s3_bucket, s3_key,
                clearinghouse, clinic_id, tenant_id,
                payer_id, payer_name, payer_address,
                payee_npi, payee_tax_id, payee_name,
                interchange_control_number, production_flag,
                payment_batches, received_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
        """,
            (
                era.get("era_id"),
                era.get("file_name"),
                era.get("s3_bucket"),
                era.get("s3_key"),
                source.get("clearinghouse"),
                source.get("clinic_id"),
                source.get("tenant_id"),
                payer.get("payer_id"),
                payer.get("payer_name"),
                json.dumps(payer.get("payer_address", {})),
                payee.get("npi"),
                payee.get("tax_id"),
                payee.get("provider_name"),
                era.get("interchange_control_number"),
                era.get("production_flag"),
                json.dumps(era.get("payment_batches", [])),
                era.get("received_at") or None,
            ),
        )
        conn.commit()
        logger.info("ERA inserted: %s", era.get("era_id"))
    except Exception as e:
        conn.rollback()
        logger.error("ERA insert failed: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()
