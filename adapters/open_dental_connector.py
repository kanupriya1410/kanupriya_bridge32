import json
import logging
import os

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE", "opendental"),
    )


def show_tables() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables


def fetch_patients(limit: int = 10) -> list:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT
            PatNum       AS patient_id,
            FName        AS first_name,
            LName        AS last_name,
            Birthdate    AS dob,
            Gender       AS gender,
            Address      AS address,
            City         AS city,
            State        AS state,
            Zip          AS zip
        FROM patient
        LIMIT %s
        """,
        (limit,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "patient_id": str(r["patient_id"]),
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "dob": str(r["dob"]) if r["dob"] else None,
            "gender": r["gender"],
            "address": r["address"],
            "city": r["city"],
            "state": r["state"],
            "zip": r["zip"],
        }
        for r in rows
    ]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    log.info("=" * 40)
    log.info("Connecting to Open Dental MySQL...")
    log.info("=" * 40)

    try:
        conn = get_connection()
        log.info("Connected! Server: %s", conn.get_server_info())
        conn.close()

        tables = show_tables()
        log.info("Found %s tables in opendental DB", len(tables))

        log.info("Fetching patients...")
        patients = fetch_patients(limit=5)
        log.info("Found %s patients", len(patients))
        for p in patients:
            log.info("%s", json.dumps(p, indent=2))

    except Exception as e:
        log.error("Error: %s", e)
