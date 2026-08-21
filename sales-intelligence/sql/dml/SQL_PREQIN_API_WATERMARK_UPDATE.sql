-- ============================================================
-- 03 · Preqin API Watermark Update
-- ============================================================

UPDATE IDENTIFIER(:catalog || '.' || :schema || '.preqin_api_watermark')
SET
    last_watermark_value = IF(:status = 'SUCCESS' AND :last_watermark_value != '', :last_watermark_value, last_watermark_value)
WHERE target_table = :target_table;