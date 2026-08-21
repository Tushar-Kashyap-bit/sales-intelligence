# Silver SCD2 · Incremental Endpoints
# Applies SCD Type 2 CDC to incremental Preqin bronze tables.
# Preqin ACTION column (INSERTED/UPDATED/DELETED) drives merge logic.
# snapshot_date is used as the SCD2 sequence/version column.

import pyspark.pipelines as dp
from pyspark.sql import functions as F

_TABLE_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.enableTypeWidening": "true",
}

# Excluded from silver target — technical/meta columns not needed in silver layer
_EXCLUDE_FROM_TARGET = ["_source_file", "_rescued_data", "ACTION", "load_dts", "snapshot_date"]

config_catalog = spark.conf.get("pipelines.config_catalog", "dev")
source_schema = spark.conf.get("pipelines.bronze_schema", "bronze_preqin")
target_schema = spark.conf.get("pipelines.silver_schema", "silver_preqin")
pipeline_domains = [d.strip() for d in spark.conf.get("pipelines.pipeline_domains", "").split(",") if d.strip()]

print(f"INFO: Pipeline config - catalog: '{config_catalog}', bronze: '{source_schema}', silver: '{target_schema}', domains: {pipeline_domains}")

all_endpoints = (
    spark.read.table(f"{config_catalog}.{source_schema}.preqin_api_watermark")
    .filter("is_active = true AND is_incremental = true AND primary_key_columns IS NOT NULL")
    .filter(F.col("api_domain").isin(pipeline_domains) if pipeline_domains else F.lit(True))
    .select("target_table", "primary_key_columns", "api_domain")
    .collect()
)

def _register_streaming_view(view_name: str, source_table: str, pk_list: list) -> None:
    """Drops rows with NULL primary keys — NULL keys corrupt SCD2 merge.
    Ensures ACTION column exists for consistent CDC handling."""
    pk_not_null = {
        f"{col.strip().replace(' ', '_').lower()}_not_null": f"`{col.strip()}` IS NOT NULL"
        for col in pk_list
    }

    @dp.temporary_view(name=view_name)
    @dp.expect_all_or_drop(pk_not_null)
    def _():
        df = spark.readStream.table(source_table)

        if "ACTION" not in df.columns:
            df = df.withColumn("ACTION", F.lit(None).cast("string"))

        return df

for row in all_endpoints:
    pk_list = [c.strip() for c in row.primary_key_columns.split(",")]
    source_table = f"{config_catalog}.{source_schema}.{row.target_table}"
    target_table = f"{config_catalog}.{target_schema}.{row.target_table}"
    streaming_view = f"{row.target_table}_bronze_stream"

    try:
        spark.read.table(source_table).schema

        _register_streaming_view(
            streaming_view,
            source_table,
            pk_list
        )

        dp.create_streaming_table(
            name=target_table,
            table_properties=_TABLE_PROPERTIES
        )

        dp.create_auto_cdc_flow(
            source=streaming_view,
            target=target_table,
            keys=pk_list,
            sequence_by=F.col("snapshot_date"),
            except_column_list=_EXCLUDE_FROM_TARGET,
            stored_as_scd_type=2,
            apply_as_deletes=F.upper(F.col("ACTION")) == "DELETED",
        )

        print(f"  Registered: {row.target_table}  (PKs: {row.primary_key_columns})")

    except Exception as e:
        print(f"  Error registering {row.target_table}: {e}")