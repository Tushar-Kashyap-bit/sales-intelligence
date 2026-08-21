-- ============================================================
-- Salesforce / Source History + Silver Framework
-- ============================================================

DECLARE OR REPLACE VARIABLE catalog STRING DEFAULT '${var.catalog}';
DECLARE OR REPLACE VARIABLE source_schema STRING DEFAULT 'bronze_salesforce';
DECLARE OR REPLACE VARIABLE history_schema STRING DEFAULT 'bronze_salesforce_history';
DECLARE OR REPLACE VARIABLE silver_schema STRING DEFAULT 'silver_salesforce';


-- ============================================================
-- 1. Create schemas
-- ============================================================

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(
    catalog || '.' || history_schema
);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(
    catalog || '.' || silver_schema
);


-- ============================================================
-- 2. Define source tables
--
-- Add/remove table names here only.
-- No need to create separate SQL for every table.
-- ============================================================

DECLARE OR REPLACE VARIABLE table_list ARRAY<STRING> DEFAULT ARRAY(
    'account',
    'contact',
    'funds_available__c',
    'opportunity',
    'recordtype',
    'reit_investment__c'
);


-- ============================================================
-- 3. Create History tables
-- ============================================================

FOR table_name IN table_list DO

    EXECUTE IMMEDIATE '
        CREATE TABLE IF NOT EXISTS ' ||
        catalog || '.' || history_schema || '.' ||
        table_name || '_history
        AS
        SELECT *
        FROM ' ||
        catalog || '.' || source_schema || '.' ||
        table_name || '
        WHERE 1 = 0
    ';

END FOR;


-- ============================================================
-- 4. Create Silver Views
--
-- History records with END timestamp
-- UNION ALL BY NAME
-- Current Bronze records
-- ============================================================

FOR table_name IN table_list DO

    EXECUTE IMMEDIATE '
        CREATE OR REPLACE VIEW ' ||
        catalog || '.' || silver_schema || '.' ||
        table_name || '
        AS

        SELECT *
        FROM ' ||
        catalog || '.' || history_schema || '.' ||
        table_name || '_history
        WHERE __END_AT IS NOT NULL

        UNION

        SELECT *
        FROM ' ||
        catalog || '.' || source_schema || '.' ||
        table_name || '
    ';

END FOR;