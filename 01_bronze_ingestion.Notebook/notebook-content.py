# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f7b20d3e-dbcf-42ff-91be-8ca1fb0ece86",
# META       "default_lakehouse_name": "lh_procurement_analytics",
# META       "default_lakehouse_workspace_id": "073a66be-663a-49a6-8033-dde8dcf9c645",
# META       "known_lakehouses": [
# META         {
# META           "id": "f7b20d3e-dbcf-42ff-91be-8ca1fb0ece86"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: 01_bronze_ingestion
# Lakehouse: lh_procurement_analytics  |  Stage: bronze
# Source: Data Pipeline "pl_bronze_copy" (Copy Job, one activity per source file)
#   landing zone -> Files/bronze/<source>/ -> this notebook reads + writes Delta tables
#
# 8 sources: CD, CX, MDB, MM, MOBO15, MOBO25, MS, SI
# MM is ingested ONLY as a back-test reference (Week 3) - not used to build gold movement_status.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_FILES = "Files/bronze"
SOURCES = {
    "cd":      {"path": f"{BRONZE_FILES}/CD.csv",      "header": True},
    "cx":      {"path": f"{BRONZE_FILES}/CX.csv",      "header": True},
    "mdb":     {"path": f"{BRONZE_FILES}/MDB.csv",     "header": True},
    "mm":      {"path": f"{BRONZE_FILES}/MM.csv",      "header": True},   # back-test reference only
    "mobo15":  {"path": f"{BRONZE_FILES}/MOBO15.csv",  "header": True},
    "ms":      {"path": f"{BRONZE_FILES}/MS.csv",      "header": True},
    "si":      {"path": f"{BRONZE_FILES}/SI.csv",      "header": True},
    # MOBO25 is .xlsx - land via Dataflow Gen2 (Power Query) instead of Copy Job, see below
}

for name, cfg in SOURCES.items():
    df = (
        spark.read
        .option("header", cfg["header"])
        .option("inferSchema", "false")  # bronze = raw strings, typed in silver
        .option("encoding", "UTF-8")
        .csv(cfg["path"])
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(cfg["path"]))
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze_{name}")
    )
    print(f"bronze_{name}: {df.count()} rows")

# --- MOBO25.xlsx ---
# 36MB .xlsx, current-stock snapshot. Land via Dataflow Gen2 (Power Query "Get Data from Excel")
# -> bronze_mobo25 Delta table. Power Query handles the binary/Excel parsing that Copy Job
# (CSV/JSON/Parquet-oriented) does not support natively. No transform logic in the dataflow -
# 1:1 column passthrough + _ingested_at/_source_file columns to match the pattern above.

# --- Row-count parity check (Week 2 deliverable) ---
# Compare bronze table counts vs source file row counts (excluding header).
# Record results in logs/troubleshooting_log.md "Bronze parity report".


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re

df_mobo25 = pd.read_excel("/lakehouse/default/Files/bronze/MOBO25.xlsx")
df_mobo25["_ingested_at"] = pd.Timestamp.now()
df_mobo25["_source_file"] = "Files/bronze/MOBO25.xlsx"

# Clean column names - replace spaces and special chars with underscores
df_mobo25.columns = [re.sub(r'[ ,;{}()\n\t=]', '_', col) for col in df_mobo25.columns]

spark_df = spark.createDataFrame(df_mobo25.astype(str))
(
    spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_mobo25")
)
print(f"bronze_mobo25: {spark_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
