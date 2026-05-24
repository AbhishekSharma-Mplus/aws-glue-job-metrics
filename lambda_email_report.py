import csv
import io
import json
import logging
import os
import smtplib
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SECRET_NAME      = os.environ["SECRET_NAME"]       # DB secret (shared with ingest Lambda)
SMTP_SECRET_NAME = os.environ["SMTP_SECRET_NAME"]  # Proofpoint SMTP secret
DB_NAME          = os.environ.get("DB_NAME")        # optional DB name override

# Columns written to the CSV, in display order
CSV_COLUMNS = [
    "job_name",
    "total_runs",
    "success_count",
    "failure_count",
    "last_run_state",
    "last_run_time",
    "avg_execution_time_seconds",
    "last_error_message",
]

# Query: last-7-day metrics per job, ordered by most-failed first
METRICS_QUERY = """
WITH base AS (
    SELECT *
    FROM glue_job_runs
    WHERE started_on >= NOW() - INTERVAL '7 days'
),
latest AS (
    SELECT DISTINCT ON (job_name)
        job_name,
        job_state   AS last_run_state,
        started_on  AS last_run_time,
        error_message
    FROM base
    ORDER BY job_name, started_on DESC
)
SELECT
    b.job_name,
    COUNT(*)                                              AS total_runs,
    COUNT(*) FILTER (WHERE b.job_state = 'SUCCEEDED')    AS success_count,
    COUNT(*) FILTER (WHERE b.job_state = 'FAILED')       AS failure_count,
    l.last_run_state,
    l.last_run_time,
    ROUND(AVG(b.execution_time_seconds)::NUMERIC, 2)     AS avg_execution_time_seconds,
    l.error_message                                       AS last_error_message
FROM base b
JOIN latest l USING (job_name)
GROUP BY b.job_name, l.last_run_state, l.last_run_time, l.error_message
ORDER BY failure_count DESC, total_runs DESC;
"""


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------

def get_db_credentials(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")
    secret = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
    return {
        "host":     secret["host"],
        "port":     int(secret.get("port", 5432)),
        "user":     secret["username"],
        "password": secret["password"],
        "dbname":   DB_NAME or secret["dbname"],
    }


def get_smtp_config(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")
    secret = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
    recipients = secret["recipients"]
    if isinstance(recipients, str):
        recipients = [r.strip() for r in recipients.split(",") if r.strip()]
    return {
        "host":         secret["host"],
        "port":         int(secret.get("port", 587)),
        "username":     secret["username"],
        "password":     secret["password"],
        "from_address": secret["from_address"],
        "recipients":   recipients,
    }


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def fetch_metrics(conn) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(METRICS_QUERY)
        return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Email content builders
# ---------------------------------------------------------------------------

def build_summary_html(rows: list, report_date: str) -> str:
    total_runs    = sum(r["total_runs"]    for r in rows)
    total_success = sum(r["success_count"] for r in rows)
    total_failure = sum(r["failure_count"] for r in rows)
    success_rate  = (total_success / total_runs * 100) if total_runs else 0

    top5 = rows[:5]  # already sorted by failure_count DESC

    top5_rows_html = ""
    for r in top5:
        top5_rows_html += (
            f"<tr>"
            f"<td style='padding:6px 12px;border:1px solid #ddd'>{r['job_name']}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd;text-align:center'>{r['total_runs']}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd;text-align:center;color:#27ae60'>{r['success_count']}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd;text-align:center;color:#e74c3c'>{r['failure_count']}</td>"
            f"<td style='padding:6px 12px;border:1px solid #ddd;text-align:center'>{r['last_run_state']}</td>"
            f"</tr>"
        )

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;margin:0;padding:20px">
      <h2 style="color:#2c3e50">AWS Glue Job Report &mdash; {report_date}</h2>
      <p>Rolling 7-day window ending {report_date}.</p>

      <h3 style="color:#2c3e50">Summary</h3>
      <table style="border-collapse:collapse;margin-bottom:24px">
        <tr>
          <td style="padding:8px 20px;background:#f4f6f7;border:1px solid #ddd"><strong>Total Job Runs</strong></td>
          <td style="padding:8px 20px;border:1px solid #ddd">{total_runs}</td>
        </tr>
        <tr>
          <td style="padding:8px 20px;background:#f4f6f7;border:1px solid #ddd"><strong>Successful Runs</strong></td>
          <td style="padding:8px 20px;border:1px solid #ddd;color:#27ae60"><strong>{total_success}</strong></td>
        </tr>
        <tr>
          <td style="padding:8px 20px;background:#f4f6f7;border:1px solid #ddd"><strong>Failed Runs</strong></td>
          <td style="padding:8px 20px;border:1px solid #ddd;color:#e74c3c"><strong>{total_failure}</strong></td>
        </tr>
        <tr>
          <td style="padding:8px 20px;background:#f4f6f7;border:1px solid #ddd"><strong>Overall Success Rate</strong></td>
          <td style="padding:8px 20px;border:1px solid #ddd">{success_rate:.1f}%</td>
        </tr>
        <tr>
          <td style="padding:8px 20px;background:#f4f6f7;border:1px solid #ddd"><strong>Distinct Jobs</strong></td>
          <td style="padding:8px 20px;border:1px solid #ddd">{len(rows)}</td>
        </tr>
      </table>

      <h3 style="color:#2c3e50">Top 5 Most-Failed Jobs</h3>
      <table style="border-collapse:collapse;margin-bottom:24px">
        <thead>
          <tr style="background:#2c3e50;color:#fff">
            <th style="padding:8px 12px;border:1px solid #ddd">Job Name</th>
            <th style="padding:8px 12px;border:1px solid #ddd">Total Runs</th>
            <th style="padding:8px 12px;border:1px solid #ddd">Successes</th>
            <th style="padding:8px 12px;border:1px solid #ddd">Failures</th>
            <th style="padding:8px 12px;border:1px solid #ddd">Last State</th>
          </tr>
        </thead>
        <tbody>
          {top5_rows_html}
        </tbody>
      </table>

      <p style="color:#7f8c8d;font-size:12px">
        Full metrics are attached as a CSV file ordered by failure count (highest first).<br>
        This report is generated automatically by AWS Lambda.
      </p>
    </body></html>
    """
    return html


def build_csv_bytes(rows: list) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        # Normalise datetime objects to ISO strings for CSV readability
        sanitised = dict(row)
        if sanitised.get("last_run_time") is not None:
            sanitised["last_run_time"] = str(sanitised["last_run_time"])
        writer.writerow(sanitised)
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# Email sender
# ---------------------------------------------------------------------------

def send_email(smtp_config: dict, subject: str, html_body: str, csv_bytes: bytes, csv_filename: str) -> None:
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = smtp_config["from_address"]
    msg["To"]      = ", ".join(smtp_config["recipients"])

    msg.attach(MIMEText(html_body, "html"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{csv_filename}"')
    msg.attach(part)

    with smtplib.SMTP(smtp_config["host"], smtp_config["port"], timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_config["username"], smtp_config["password"])
        server.sendmail(
            smtp_config["from_address"],
            smtp_config["recipients"],
            msg.as_string(),
        )

    logger.info("Email sent to %s", smtp_config["recipients"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Triggered daily by an EventBridge cron rule.
    Reads 7-day Glue metrics from RDS, builds an HTML email + CSV attachment,
    and delivers via Proofpoint SMTP.
    """
    today        = date.today().isoformat()          # e.g. "2026-05-24"
    csv_filename = f"glue_job_report_{today}.csv"
    subject      = f"AWS Glue Job Report — {today}"

    db_creds    = get_db_credentials(SECRET_NAME)
    smtp_config = get_smtp_config(SMTP_SECRET_NAME)

    conn = None
    try:
        conn = psycopg2.connect(**db_creds)
        rows = fetch_metrics(conn)
        logger.info("Fetched metrics for %d job(s)", len(rows))

        if not rows:
            logger.warning("No job runs found in the last 7 days — skipping email.")
            return {"statusCode": 200, "body": "No data"}

        html_body = build_summary_html(rows, today)
        csv_bytes = build_csv_bytes(rows)

        send_email(smtp_config, subject, html_body, csv_bytes, csv_filename)
        logger.info("Report delivered: %s (%d jobs, %d bytes CSV)", today, len(rows), len(csv_bytes))

        return {"statusCode": 200, "body": f"Report sent for {today}"}

    finally:
        if conn is not None:
            conn.close()
