# aws-glue-job-metrics
Two AWS Lambda functions that capture AWS Glue job run metadata into RDS PostgreSQL and deliver a daily email report via Proofpoint SMTP.

## Repository structure

```
glue-job-ops-monitor/
├── lambda_function.py        # Lambda 1 — Glue state ingest → RDS
├── lambda_email_report.py    # Lambda 2 — Daily email report via SMTP
└── README.md
```

---

## Overview

| Lambda | File | Trigger | Purpose |
|---|---|---|---|
| **Glue Ingest** | `lambda_function.py` | EventBridge — Glue Job State Change | Captures every Glue job run into RDS PostgreSQL in real time |
| **Daily Report** | `lambda_email_report.py` | EventBridge — daily cron `0 7 * * ? *` | Aggregates 7-day metrics and sends an HTML email + CSV via Proofpoint SMTP |

---

## Lambda 1 — Glue Job State Ingest (`lambda_function.py`)

### What it does

Fires on every AWS Glue job state change (SUCCEEDED, FAILED, RUNNING, STOPPED, etc.) via an EventBridge rule. It fetches the full job run metadata from the Glue API and upserts it into a PostgreSQL table.

### Trigger — EventBridge rule pattern

```json
{
  "source": ["aws.glue"],
  "detail-type": ["Glue Job State Change"]
}
```

### PostgreSQL table (auto-created on first run)

```sql
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
```

The schema is driven by `SCHEMA_MAPPING` at the top of `lambda_function.py`. Add or remove fields there without touching any other code.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `SECRET_NAME` | Yes | Secrets Manager secret name for RDS credentials |
| `DB_NAME` | No | Overrides the `dbname` field in the secret |

### Secrets Manager — DB secret (`SECRET_NAME`)

```json
{
  "host": "mydb.xxxx.us-east-1.rds.amazonaws.com",
  "port": 5432,
  "username": "lambda_user",
  "password": "...",
  "dbname": "glue_metadata"
}
```

### IAM permissions required

```
glue:GetJobRun
secretsmanager:GetSecretValue   (scoped to DB secret ARN)
```

Plus VPC network access to the RDS instance (same VPC/subnets/security group).

### Key design notes

- **Idempotent upsert** — `ON CONFLICT (job_run_id) DO UPDATE` means replaying an event is safe.
- **Schema-driven** — `SCHEMA_MAPPING` dict is the single source of truth for field names and type coercions.
- **Auto DDL** — `ensure_table()` runs `CREATE TABLE IF NOT EXISTS` on every cold start; no-op after the first time.

---

## Lambda 2 — Daily Email Report (`lambda_email_report.py`)

### What it does

Runs once a day, queries the `glue_job_runs` table for the last 7 days, and sends an email via Proofpoint SMTP containing:

1. **HTML body** — summary snapshot (total runs, success/failure counts, success rate, top-5 most-failed jobs table).
2. **CSV attachment** — full job metrics for all jobs, ordered by `failure_count DESC` (highest-priority/most-failed first).

### Trigger — EventBridge scheduled rule

```
cron(0 7 * * ? *)   →  daily at 07:00 UTC
```

### Email output

**Body includes:**
- Total runs, successful runs, failed runs, overall success rate
- Distinct job count
- Top-5 most-failed jobs table

**CSV attachment columns** (`glue_job_report_YYYY-MM-DD.csv`):

| Column | Description |
|---|---|
| `job_name` | Glue job name |
| `total_runs` | Total runs in the 7-day window |
| `success_count` | Number of SUCCEEDED runs |
| `failure_count` | Number of FAILED runs (sort key) |
| `last_run_state` | State of the most recent run |
| `last_run_time` | Timestamp of the most recent run |
| `avg_execution_time_seconds` | Average duration across all runs |
| `last_error_message` | Error message from the most recent failed run |

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `SECRET_NAME` | Yes | Same RDS secret as Lambda 1 |
| `SMTP_SECRET_NAME` | Yes | Secrets Manager secret name for Proofpoint SMTP |
| `DB_NAME` | No | Overrides the `dbname` field in the DB secret |

### Secrets Manager — SMTP secret (`SMTP_SECRET_NAME`)

```json
{
  "host": "smtp.proofpoint.example.com",
  "port": 587,
  "username": "smtp-user@company.com",
  "password": "...",
  "from_address": "glue-reports@company.com",
  "recipients": ["team@company.com", "oncall@company.com"]
}
```

`recipients` can be a JSON array or a comma-separated string.

### IAM permissions required

```
secretsmanager:GetSecretValue   (scoped to DB secret ARN)
secretsmanager:GetSecretValue   (scoped to SMTP secret ARN)
```

No Glue permissions needed — reads only from the PostgreSQL table.

---

## Shared dependency — psycopg2 Lambda layer

Neither Lambda ships `psycopg2` in the standard runtime. Bundle it as a Lambda layer:

```bash
pip install psycopg2-binary -t python/
zip -r psycopg2-layer.zip python/
```

Then attach the layer to both Lambda functions. All other imports (`smtplib`, `csv`, `email`, `json`, `io`) are Python stdlib.

---

## Deployment checklist

- [ ] Create the RDS PostgreSQL instance (or use an existing one) in a VPC
- [ ] Create the DB Secrets Manager secret
- [ ] Create the SMTP Secrets Manager secret
- [ ] Build and publish the `psycopg2-binary` Lambda layer
- [ ] Deploy **Lambda 1** (`lambda_function.py`)
  - [ ] Attach the psycopg2 layer
  - [ ] Set `SECRET_NAME` env var
  - [ ] Configure VPC (same as RDS)
  - [ ] Grant IAM: `glue:GetJobRun`, `secretsmanager:GetSecretValue`
  - [ ] Create EventBridge rule: `source: aws.glue`, `detail-type: Glue Job State Change`
- [ ] Deploy **Lambda 2** (`lambda_email_report.py`)
  - [ ] Attach the psycopg2 layer
  - [ ] Set `SECRET_NAME` and `SMTP_SECRET_NAME` env vars
  - [ ] Configure VPC (same as RDS)
  - [ ] Grant IAM: `secretsmanager:GetSecretValue` for both secrets
  - [ ] Create EventBridge scheduled rule: `cron(0 7 * * ? *)`

---

## Testing

### Lambda 1 — manual test payload

```json
{
  "source": "aws.glue",
  "detail-type": "Glue Job State Change",
  "detail": {
    "jobName": "my-etl-job",
    "jobRunId": "jr_abc123",
    "state": "SUCCEEDED"
  }
}
```

Verify with:
```sql
SELECT * FROM glue_job_runs ORDER BY recorded_at DESC LIMIT 5;
```

### Lambda 2 — manual invoke

Invoke with an empty payload `{}`. Check CloudWatch Logs for:
```
Fetched metrics for N job(s)
Email sent to [...]
Report delivered: YYYY-MM-DD (N jobs, N bytes CSV)
```

Then confirm the email arrives with a populated HTML body and a valid CSV attachment.
