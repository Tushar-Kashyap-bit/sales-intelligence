-- ---------------------------------------------------------------------------
-- Preqin API Audit Log Table Setup (Single-Run SQL Script)
-- Creates the append-only audit log for endpoint-level execution tracking.
-- Run against: dev.bronze_preqin (or replace catalog/schema as needed).
-- ---------------------------------------------------------------------------

DECLARE OR REPLACE VARIABLE catalog STRING DEFAULT '${var.catalog}';
DECLARE OR REPLACE VARIABLE bronze_schema STRING DEFAULT 'bronze_preqin';

CREATE TABLE IF NOT EXISTS IDENTIFIER(catalog || '.' || bronze_schema || '.preqin_api_audit_log') (
  api_domain            STRING      NOT NULL
                                    COMMENT 'Business domain associated with the endpoint (e.g., investors, funds, deals, esg, benchmarks).',
  endpoint_url          STRING
                                    COMMENT 'Logical name of the API endpoint being processed (e.g., Fund Managers PE, Deals INF).',
  target_table          STRING
                                    COMMENT 'Bronze Delta table loaded by this endpoint. Helps identify impacted tables during failures.',
  watermark_used        STRING
                                    COMMENT 'Watermark value used to load data when the pipeline runs.',
  status                STRING      NOT NULL
                                    COMMENT 'Execution outcome. Allowed values: RUNNING, SUCCESS, FAILED, SKIPPED, RETRYING.',
  error_message         STRING
                                    COMMENT 'Failure detail or exception message. NULL when status is SUCCESS or SKIPPED.',
  run_id                STRING
                                    COMMENT 'Child job run ID from {{job.run_id}} dynamic reference. Identifies the pipeline execution instance.',
  created_timestamp     TIMESTAMP   NOT NULL
                                    COMMENT 'Job start time from {{job.start_time.iso_datetime}} dynamic reference. Captures when the job run began.'
)
USING DELTA
COMMENT 'Endpoint-level audit log for the Preqin API ingestion framework. A new row is inserted for each significant processing event (start, success, failure, retry). Do not update existing rows — append only.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
