# Silver SCD2 · Full-Load (Snapshot)
# Applies SCD Type 2 via callable snapshot replay on full-load Bronze tables.
# Replays each snapshot_date in order — preserves __START_AT / __END_AT.
# snapshot_date is used as the snapshot version column.

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_TABLE_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.enableTypeWidening": "true",
}

# Dropped before snapshot diff — prevents false change detection
_EXCLUDE_FROM_DIFF = ["load_dts", "snapshot_date", "_source_file", "_rescued_data"]

config_catalog = spark.conf.get("pipelines.config_catalog", "dev")
source_schema = spark.conf.get("pipelines.bronze_schema", "bronze_preqin")
target_schema = spark.conf.get("pipelines.silver_schema", "silver_preqin")
pipeline_domains = [d.strip() for d in spark.conf.get("pipelines.pipeline_domains", "").split(",") if d.strip()]

print(f"INFO: Pipeline config - catalog: '{config_catalog}', bronze: '{source_schema}', silver: '{target_schema}', domains: {pipeline_domains}")

all_endpoints = (
    spark.read.table(f"{config_catalog}.{source_schema}.preqin_api_watermark")
    .filter("is_active = true AND is_incremental = false AND primary_key_columns IS NOT NULL")
    .filter(F.col("api_domain").isin(pipeline_domains) if pipeline_domains else F.lit(True))
    .select("target_table", "primary_key_columns", "api_domain")
    .collect()
)

def _make_snapshot_callable(source_table: str, pk_list: list):
    """Factory returning a callable for create_auto_cdc_from_snapshot_flow.
    Replays snapshots by snapshot_date in order — on full refresh this rebuilds
    the complete SCD2 timeline preserving __START_AT / __END_AT."""

    def next_snapshot_and_version(latest_version):
        dates_df = (
            spark.read.table(source_table)
            .select(F.col("snapshot_date"))
            .filter(F.col("snapshot_date").isNotNull())
            .distinct()
            .orderBy("snapshot_date")
        )

        if latest_version is not None:
            dates_df = dates_df.filter(F.col("snapshot_date") > latest_version)

        next_row = dates_df.first()

        if next_row is None:
            return None

        next_version = next_row["snapshot_date"]

        pk_filter = " AND ".join([f"`{col.strip()}` IS NOT NULL" for col in pk_list])

        window = Window.partitionBy(*pk_list).orderBy(
            F.col("load_dts").desc(),
            F.col("_source_file").desc()
        )

        df = (
            spark.read.table(source_table)
            .filter(F.col("snapshot_date") == next_version)
            .filter(pk_filter)
            .withColumn("_dedup_rank", F.row_number().over(window))
            .filter("_dedup_rank = 1")
            .drop("_dedup_rank", *_EXCLUDE_FROM_DIFF)
        )

        return (df, next_version)

    return next_snapshot_and_version

for row in all_endpoints:
    pk_list = [c.strip() for c in row.primary_key_columns.split(",")]
    source_table = f"{config_catalog}.{source_schema}.{row.target_table}"

    try:
        target_table = f"{config_catalog}.{target_schema}.{row.target_table}"

        dp.create_streaming_table(
            name=target_table,
            table_properties=_TABLE_PROPERTIES
        )

        dp.create_auto_cdc_from_snapshot_flow(
            source=_make_snapshot_callable(source_table, pk_list),
            target=target_table,
            keys=pk_list,
            stored_as_scd_type=2,
        )

        print(f"  Registered: {row.target_table}  (PKs: {row.primary_key_columns})")

    except Exception as e:
        print(f"  Error registering {row.target_table}: {e}")