import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Schema mapping: Glue API JobRun key → (db_column, type_coercion)
# Add or remove entries here to evolve the schema without touching any other
# part of the code.
# ---------------------------------------------------------------------------
SCHEMA_MAPPING = {
    "JobName":              ("job_name",               str),
    "Id":                   ("job_run_id",              str),
    "JobRunState":          ("job_state",               str),
    "StartedOn":            ("started_on",              "ts"),
    "CompletedOn":          ("completed_on",            "ts"),
    "ExecutionTime":        ("execution_time_seconds",  float),
    "ErrorMessage":         ("error_message",           str),
    "MaxCapacity":          ("max_capacity",            float),
    "AllocatedCapacity":    ("allocated_capacity",      int),
    "DPUSeconds":           ("dpu_seconds",             float),
}

DDL = """
CREATE TABLE IF NOT EXISTS glue_job_runs (
    id                      SERIAL        PRIMARY KEY,
    job_name                VARCHAR(255)  NOT NULL,
    job_run_id              VARCHAR(255)  NOT NULL UNIQUE,
    job_state               VARCHAR(50)   NOT NULL,
    started_on              TIMESTAMPTZ,
    completed_on            TIMESTAMPTZ,
    execution_time_seconds  NUMERIC(12,2),
    error_message           TEXT,
    max_capacity            NUMERIC(8,2),
    allocated_capacity      INTEGER,
    dpu_seconds             NUMERIC(14,4),
    recorded_at             TIMESTAMPTZ   DEFAULT NOW()
);
"""

SECRET_NAME = os.environ["SECRET_NAME"]   # e.g. "prod/rds/glue-tracker"
DB_NAME     = os.environ.get("DB_NAME")   # overrides secret's dbname if set


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_db_credentials(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return {
        "host":     secret["host"],
        "port":     int(secret.get("port", 5432)),
        "user":     secret["username"],
        "password": secret["password"],
        "dbname":   DB_NAME or secret["dbname"],
    }


def get_glue_run_metadata(job_name: str, job_run_id: str) -> dict:
    client = boto3.client("glue")
    response = client.get_job_run(JobName=job_name, RunId=job_run_id)
    return response["JobRun"]


def _coerce(value, coercion):
    """Apply type coercion; return None when value is absent."""
    if value is None:
        return None
    if coercion == "ts":
        # Glue returns datetime objects; store as ISO-8601 string for psycopg2
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value
        return value  # already a string or None
    try:
        return coercion(value)
    except (ValueError, TypeError):
        return None


def build_row(job_run: dict) -> dict:
    row = {}
    for glue_key, (col, coercion) in SCHEMA_MAPPING.items():
        row[col] = _coerce(job_run.get(glue_key), coercion)
    return row


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def upsert_row(conn, row: dict) -> None:
    columns = list(row.keys())
    values  = [row[c] for c in columns]

    # Build the ON CONFLICT update clause for all columns except the unique key
    update_cols = [c for c in columns if c != "job_run_id"]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO glue_job_runs ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (job_run_id) DO UPDATE SET
            {update_clause},
            recorded_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, [values])
    conn.commit()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Expected EventBridge event shape (Glue Job State Change):
    {
        "source": "aws.glue",
        "detail-type": "Glue Job State Change",
        "detail": {
            "jobName": "my-etl-job",
            "jobRunId": "jr_abc123",
            "state": "SUCCEEDED"
        }
    }
    """
    logger.info("Received event: %s", json.dumps(event))

    detail      = event.get("detail", {})
    job_name    = detail.get("jobName")
    job_run_id  = detail.get("jobRunId")

    if not job_name or not job_run_id:
        raise ValueError(f"Missing jobName or jobRunId in event detail: {detail}")

    creds = get_db_credentials(SECRET_NAME)
    conn  = None
    try:
        conn     = psycopg2.connect(**creds)
        ensure_table(conn)

        job_run  = get_glue_run_metadata(job_name, job_run_id)
        row      = build_row(job_run)

        logger.info("Upserting row for job_run_id=%s state=%s", row["job_run_id"], row["job_state"])
        upsert_row(conn, row)
        logger.info("Done.")

        return {"statusCode": 200, "body": f"Upserted {job_run_id}"}

    finally:
        if conn is not None:
            conn.close()
