# Bronze Auto Loader
# Reads pipe-delimited CSVs from the UC Volume landing zone into Bronze Delta tables.
# Endpoints discovered dynamically from `preqin_api_watermark`.

import pyspark.pipelines as dp
from pyspark.sql import functions as F

_TABLE_PROPERTIES = {
    # Column mapping required: CSV headers have spaces/special chars (e.g. "DEAL SIZE (MN)")
    "delta.columnMapping.mode": "name",
}

source_catalog = spark.conf.get("pipelines.config_catalog", "dev")
target_schema = spark.conf.get("pipelines.config_schema", "bronze_preqin")
pipeline_domains = [d.strip() for d in spark.conf.get("pipelines.pipeline_domains", "").split(",") if d.strip()]

print(f"INFO: Pipeline config - catalog: '{source_catalog}', schema: '{target_schema}', domains: {pipeline_domains}")

endpoints = spark.read.table(f"{source_catalog}.{target_schema}.preqin_api_watermark")\
        .filter("is_active = true")\
        .filter(F.col("api_domain").isin(pipeline_domains))\
        .select("api_domain", "target_table").collect()

def _register_table(table_name: str, volume_path: str) -> None:
    """Streaming Auto Loader — appends new CSV files each run."""

    @dp.table(name=table_name, table_properties=_TABLE_PROPERTIES)
    def _():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header", "true")
            .option("delimiter", "|")
            .option("multiLine", "true")
            .load(volume_path)
            .withColumns({
                "load_dts": F.current_timestamp(),
                "_source_file": F.col("_metadata.file_path"),
                "snapshot_date": F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"_(\d{8})\.csv$", 1), "yyyyMMdd"),
            })
        )

for row in endpoints:
    vpath = f"/Volumes/{source_catalog}/{target_schema}/preqin_landing/{row.api_domain}/{row.target_table}"

    try:
        files = dbutils.fs.ls(vpath)
        if files:
            print(f"INFO: Registering table '{row.target_table}' from '{vpath}' ({len(files)} file(s) found)")
            _register_table(row.target_table, vpath)
        else:
            print(f"ERROR: No files found in '{vpath}' for table '{row.target_table}'. Skipping.")
    except Exception as e:
        print(f"ERROR: '{vpath}' not accessible for table '{row.target_table}': {e}. Skipping.")