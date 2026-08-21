-- ============================================================
-- 04 · Insert Audit Log Entry
-- ============================================================

INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.preqin_api_audit_log')
(
    api_domain,
    endpoint_url,
    target_table,
    watermark_used,
    status,
    error_message,
    run_id,
    created_timestamp
)
VALUES (
    :domain,
    :endpoint_url,
    :target_table,
    :watermark_value_used,
    :status,
    :error_message,
    :run_id,
    :created_timestamp
);
