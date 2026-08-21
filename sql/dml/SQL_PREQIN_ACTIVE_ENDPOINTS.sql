-- ============================================================
-- 01 · Get Active Endpoints
-- Drives the For Each task — one row per active endpoint.
-- Each row is passed to 02_extract_to_volume as endpoint_data.
-- Parameters: :catalog, :schema (set at job level)
-- ============================================================

SELECT
  endpoint_url,
  api_domain,
  api_version_used,
  is_incremental,
  target_table,
  last_watermark_value
FROM IDENTIFIER(:catalog || '.' || :schema || '.preqin_api_watermark')
WHERE is_active = TRUE
ORDER BY api_domain, target_table;