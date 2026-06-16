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

# Fabric Notebook: 02_silver_clean
# Lakehouse: lh_procurement_analytics  |  Stage: bronze -> silver
# Cleans known data-quality issues per project_plan_fabric.md Week 3 and
# data_dictionary_assumptions_log.md before any gold-layer joins.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

spark = SparkSession.builder.getOrCreate()

# --- MOBO15: fact_movements source ---
# Issues: column is "posring_date" (typo for posting_date); some rows contain
# the literal string "Not_found" in posring_date (23,580 of ~ total - corrupt
# rows from source export). document_date/entry_date columns documented but
# do not exist - not referenced anywhere downstream.
mobo15 = spark.table("bronze_mobo15")

silver_mobo15 = (
    mobo15
    .withColumnRenamed("posring_date", "posting_date_raw")
    .withColumn(
        "posting_date",
        F.when(F.col("posting_date_raw") == "Not_found", None)
         .otherwise(F.to_date(F.col("posting_date_raw"), "dd/MM/yyyy"))
    )
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("_is_unparsed_date", F.col("posting_date_raw") == "Not_found")
    .drop("posting_date_raw")
)
(silver_mobo15.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_mobo15"))

n_total = silver_mobo15.count()
n_bad = silver_mobo15.filter("_is_unparsed_date = true").count()
print(f"silver_mobo15: {n_total} rows, {n_bad} with unparsable posting_date "
      f"(retained, flagged _is_unparsed_date - excluded from 36-month window calcs)")

# --- MM: back-test reference only ---
# Issue: column "aveverage_quantity_12" is a typo for "average_quantity_12".
mm = spark.table("bronze_mm")
silver_mm = (
    mm
    .withColumnRenamed("aveverage_quantity_12", "average_quantity_12")
    .withColumn("average_issue_6", F.col("average_issue_6").cast(IntegerType()))
    .withColumn("average_issue_12", F.col("average_issue_12").cast(IntegerType()))
    .withColumn("average_quantity_6", F.col("average_quantity_6").cast(IntegerType()))
    .withColumn("average_quantity_12", F.col("average_quantity_12").cast(IntegerType()))
    .withColumn("maximum_quantity", F.col("maximum_quantity").cast(IntegerType()))
    .withColumn("calculated_maximum", F.col("calculated_maximum").cast(IntegerType()))
)
(silver_mm.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_mm"))
print(f"silver_mm: {silver_mm.count()} rows")

# --- MOBO25: fact_stock source (landed via pandas from .xlsx) ---
# Issue: blank item_description rows - confirmed expected (not a load error), retain as-is.
#
# Column-name note: notebook 01's regex replaced spaces -> underscores but left dots and
# hyphens intact, producing names like "In_Quality_Insp." and "Restricted-Use_Stock".
# Dots cause Spark to interpret the name as a struct path; hyphens cause parse errors.
# Fix: re-clean ALL column names here (replace any non-alphanumeric-non-underscore char
# with underscore) before casting, so silver_mobo25 has fully safe Delta column names.

import re as _re

mobo25 = spark.table("bronze_mobo25")

def _safe_col_name(name: str) -> str:
    """Replace any char that is not [a-zA-Z0-9_] with underscore."""
    return _re.sub(r'[^a-zA-Z0-9_]', '_', name)

mobo25_recleaned = mobo25
for _old in mobo25.columns:
    _new = _safe_col_name(_old)
    if _new != _old:
        mobo25_recleaned = mobo25_recleaned.withColumnRenamed(_old, _new)

# After re-clean, the affected column names become:
#   In_Quality_Insp.    -> In_Quality_Insp_
#   Restricted-Use_Stock -> Restricted_Use_Stock
# All others are unchanged (already safe from notebook 01 cleaning).

silver_mobo25 = (
    mobo25_recleaned
    .withColumn("Unrestricted",        F.col("Unrestricted").cast(DoubleType()))
    .withColumn("In_Quality_Insp_",    F.col("In_Quality_Insp_").cast(DoubleType()))
    .withColumn("Restricted_Use_Stock",F.col("Restricted_Use_Stock").cast(DoubleType()))
    .withColumn("Blocked",             F.col("Blocked").cast(DoubleType()))
    .withColumn("Value_Unrestricted",  F.col("Value_Unrestricted").cast(DoubleType()))
)
(silver_mobo25.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_mobo25"))
print(f"silver_mobo25: {silver_mobo25.count()} rows")

# --- MS, MDB, SI, CD, CX: typed passthrough (no known quality issues beyond UTF-8 BOM,
# which Spark's csv reader on bronze already handled via header inference) ---
for name, int_cols in {
    "ms": ["minimum_level", "reorder_point", "fixed_lot", "maximum_level"],
    "mdb": [],
    "si": ["id", "country_id"],
    "cd": ["country_id"],
    "cx": ["sid"],
}.items():
    df = spark.table(f"bronze_{name}")
    for c in int_cols:
        df = df.withColumn(c, F.col(c).cast(IntegerType()))
    if name == "cx":
        df = df.withColumn("rate", F.col("rate").cast(DoubleType()))
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"silver_{name}"))
    print(f"silver_{name}: {df.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
