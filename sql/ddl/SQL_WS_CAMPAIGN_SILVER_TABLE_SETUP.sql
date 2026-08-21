-- ============================================================================
-- DDL: Silver WalletShare Campaign Tables
-- ============================================================================
-- Parameters (set via Databricks widgets or session variables):

DECLARE OR REPLACE VARIABLE catalog STRING DEFAULT '${var.catalog}';
DECLARE OR REPLACE VARIABLE silver_schema STRING DEFAULT 'silver_WS_campaign';
-- ============================================================================

-- --------------------------------------------------------------------------
-- 0. Create Schema (if not exists)
-- --------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema)
COMMENT 'Silver layer — WalletShare campaign tables enriched from bronze and Salesforce';

-- --------------------------------------------------------------------------
-- 1. pwfirm_attributes
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema || '.pwfirm_attributes')
(
  firm_crd                      STRING    COMMENT 'CRD number for dealer firm',
  institution_name              STRING    COMMENT 'Account relationship name from Salesforce',
  institution_record_type       STRING    COMMENT 'Institution Record Type',
  institution_id_18digit        STRING    COMMENT '18-character case-insensitive Salesforce ID',
  funds_available               STRING    COMMENT 'Funds available flag from Salesforce',
  `total_assets_$mil`           DOUBLE    COMMENT 'Total assets in millions (Discovery Data)',
  `total_fund_commitments_($mn)` DECIMAL(19,2) COMMENT 'Total fund commitments in millions',
  last_activity                 DATE      COMMENT 'Last contact interaction or attempt',
  last_appointment_date         DATE      COMMENT 'Last appointment date',
  lead                          STRING    COMMENT 'Owner User (OwnerId)',
  support                       STRING    COMMENT 'Internal sales support',
  `aum_$mil`                    DOUBLE    COMMENT 'AUM in millions from Salesforce',
  lead_management               STRING    COMMENT 'Lead management picklist value',
  relationship_status           STRING    COMMENT 'Relationship status',
  likelihood_of_reinvestment    STRING    COMMENT 'Likelihood of reinvestment',
  business_city                 STRING    COMMENT 'Billing city',
  business_state_province       STRING    COMMENT 'Billing state/province',
  business_country              STRING    COMMENT 'Billing country',
  phone                         STRING    COMMENT 'Phone number',
  preqin_firm_id                STRING    COMMENT 'Preqin Firm ID',
  parent_institution            STRING    COMMENT 'Parent institution (ParentId)',
  sub_region                    STRING    COMMENT 'Sub-region',
  territory                     STRING    COMMENT 'Territory',
  as_of_date                    DATE      COMMENT 'Snapshot as-of date',
  loaded_at                     TIMESTAMP COMMENT 'Row load timestamp'
)
USING DELTA
COMMENT 'Firm-level attributes from Salesforce, snapshotted per run date'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

-- --------------------------------------------------------------------------
-- 2. campaign_vehicle_1031_oz
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema || '.campaign_vehicle_1031_oz')
(
  as_of_date                    DATE          COMMENT 'As-of-date for sales and AUM figures',
  contact_id                    DECIMAL(20,0) COMMENT 'SalesConnect identifier for rep',
  crd_number                    INT           COMMENT 'CRD number for the advisor',
  asset_class                   STRING        COMMENT 'Underlying asset class for each vehicle wrapper',
  vehicle_wrapper               STRING        COMMENT 'The type of alternative investment product',
  sales_rank_90day              DOUBLE        COMMENT 'Rank based on gross sales over previous 90 days',
  sales_score_90day             DOUBLE        COMMENT 'Score based on gross sales over previous 90 days',
  sales_rank_180day             DOUBLE        COMMENT 'Rank based on gross sales over previous 180 days',
  sales_score_180day            DOUBLE        COMMENT 'Score based on gross sales over previous 180 days',
  sales_rank_12mo               DOUBLE        COMMENT 'Rank based on gross sales over previous 12 months',
  sales_score_12mo              DOUBLE        COMMENT 'Score based on gross sales over previous 12 months',
  sales_12mo                    DOUBLE        COMMENT 'Sum of gross sales in last 12 months in Alternatives',
  sales_frequency               DOUBLE        COMMENT 'Total purchases by advisor over last 12 months',
  last_purchase                 DATE          COMMENT 'Date of last purchase over last 12 months',
  max_purchase                  DOUBLE        COMMENT 'Largest purchase over last 12 months',
  new_manager                   STRING        COMMENT 'New manager flag',
  rank_aum                      DOUBLE        COMMENT 'AUM rank',
  aum_score                     DOUBLE        COMMENT 'AUM score',
  aum                           DOUBLE        COMMENT 'Total AUM held in Alternatives',
  reds_rank_90day               DOUBLE        COMMENT 'Redemption rank over previous 90 days',
  reds_score_90day              DOUBLE        COMMENT 'Redemption score over previous 90 days',
  reds_rank_180day              DOUBLE        COMMENT 'Redemption rank over previous 180 days',
  reds_score_180day             DOUBLE        COMMENT 'Redemption score over previous 180 days',
  reds_rank_12mo                DOUBLE        COMMENT 'Redemption rank over previous 12 months',
  reds_score_12mo               DOUBLE        COMMENT 'Redemption score over previous 12 months',
  reds_12mo                     DOUBLE        COMMENT 'Total redemptions in last 12 months',
  reds_frequency                DOUBLE        COMMENT 'Total redemption count over last 12 months',
  last_red                      DATE          COMMENT 'Date of last redemption over last 12 months',
  max_red                       DOUBLE        COMMENT 'Largest redemption over last 12 months',
  sales_growth_yr               DOUBLE        COMMENT 'Year-over-year sales growth',
  sales_momentum_score          DOUBLE        COMMENT 'Sales momentum score',
  `1031/oz_quintile`            INT           COMMENT 'Quintile ranking for 1031/OZ vehicle (1=top, 5=bottom)',
  loaded_at                     TIMESTAMP     COMMENT 'Row load timestamp'
)
USING DELTA
COMMENT 'Real Estate DST 1031/OZ filtered vehicle data with quintile ranking'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);

-- --------------------------------------------------------------------------
-- 3. campaign_overall
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema || '.campaign_overall')
(
  crd_number                    INT           COMMENT 'CRD number for the advisor',
  contact_id                    DECIMAL(20,0) COMMENT 'SalesConnect identifier for rep',
  firm_crd                      INT           COMMENT 'CRD number for dealer firm',
  as_of_date                    DATE          COMMENT 'As-of-date for sales and AUM figures',
  firm_id                       DECIMAL(20,0) COMMENT 'SalesConnect identifier for firm',
  office_id                     DECIMAL(20,0) COMMENT 'SalesConnect identifier for office',
  sf_18_digit_id_formula        STRING        COMMENT 'Salesforce 18-digit ID (formula field)',
  sf_18_digit_id                STRING        COMMENT 'Salesforce 18-digit record ID from contact lookup',
  firm_name                     STRING        COMMENT 'Firm name',
  funds_available               STRING        COMMENT 'Funds available flag from Salesforce',
  firm_total_commitments_mn     DECIMAL(19,2) COMMENT 'Firm total fund commitments in millions',
  Firm_Last_Activity            DATE          COMMENT 'Firm last activity date',
  Firm_Last_Appointment         DATE          COMMENT 'Firm last appointment date',
  Institution_Type              STRING        COMMENT 'Institution record type from Salesforce',
  channel_name                  STRING        COMMENT 'Firm channel (Wire, RIA, IBD, etc.)',
  territory_name                STRING        COMMENT 'Territory of the advisor',
  rep_type                      STRING        COMMENT 'Rep type (house, partnership, individual, etc.)',
  contact_name                  STRING        COMMENT 'Contact name for the advisor',
  email_address                 STRING        COMMENT 'Email address for the advisor',
  office_phone                  STRING        COMMENT 'Business phone number',
  address_line_1                STRING        COMMENT 'First address line for office',
  address_line_2                STRING        COMMENT 'Second address line for office',
  city                          STRING        COMMENT 'City of office address',
  state                         STRING        COMMENT 'State of office address',
  zip                           STRING        COMMENT 'Zip code of office address',
  relationship                  STRING        COMMENT 'Relationship status',
  sales_rank_90day              INT           COMMENT 'Sales rank over previous 90 days',
  sales_score_90day             DOUBLE        COMMENT 'Sales score over previous 90 days',
  sales_rank_180day             INT           COMMENT 'Sales rank over previous 180 days',
  sales_score_180day            DOUBLE        COMMENT 'Sales score over previous 180 days',
  sales_rank_12mo               INT           COMMENT 'Sales rank over previous 12 months',
  sales_score_12mo              DOUBLE        COMMENT 'Sales score over previous 12 months',
  sales_12mo                    DOUBLE        COMMENT 'Sum of gross sales in last 12 months',
  sales_frequency               DOUBLE        COMMENT 'Total purchases over last 12 months',
  last_purchase                 DATE          COMMENT 'Date of last purchase over last 12 months',
  max_purchase                  DOUBLE        COMMENT 'Largest purchase over last 12 months',
  new_manager                   STRING        COMMENT 'New manager flag',
  qualified_investor            DOUBLE        COMMENT 'Qualified investor indicator',
  rank_aum                      DOUBLE        COMMENT 'AUM rank',
  alt_user                      STRING        COMMENT 'Alternatives user flag (Y if rank_aum is not null)',
  aum_score                     DOUBLE        COMMENT 'AUM score',
  overall_alt_quintile          INT           COMMENT 'Overall alternatives quintile (1=top, 5=bottom)',
  aum                           DOUBLE        COMMENT 'Total AUM in Alternatives',
  reds_rank_90day               INT           COMMENT 'Redemption rank over previous 90 days',
  reds_score_90day              DOUBLE        COMMENT 'Redemption score over previous 90 days',
  reds_rank_180day              INT           COMMENT 'Redemption rank over previous 180 days',
  reds_score_180day             DOUBLE        COMMENT 'Redemption score over previous 180 days',
  reds_rank_12mo                INT           COMMENT 'Redemption rank over previous 12 months',
  reds_score_12mo               DOUBLE        COMMENT 'Redemption score over previous 12 months',
  reds_12mo                     DOUBLE        COMMENT 'Redemption total in last 12 months',
  reds_frequency                DOUBLE        COMMENT 'Redemption count over last 12 months',
  last_red                      DATE          COMMENT 'Date of last redemption over last 12 months',
  max_red                       DOUBLE        COMMENT 'Largest redemption over last 12 months',
  sales_growth_yr               DOUBLE        COMMENT 'Year-over-year sales growth',
  sales_momentum_score          DOUBLE        COMMENT 'Sales momentum score',
  asset_class                   STRING        COMMENT 'Asset class from 1031/OZ vehicle join',
  vehicle_wrapper               STRING        COMMENT 'Vehicle wrapper from 1031/OZ vehicle join',
  `1031/oz_sales_rank_12mo`     DOUBLE        COMMENT '1031/OZ specific sales rank over 12 months',
  `1031/oz_quintile`            INT           COMMENT '1031/OZ quintile ranking (1=top, 5=bottom)',
  loaded_at                     TIMESTAMP     COMMENT 'Row load timestamp'
)
USING DELTA
COMMENT 'Enriched overall WalletShare campaign with firm attributes, contact identity, and quintile rankings'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);
