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

# Fabric Notebook: 03_gold_dims
# Lakehouse: lh_procurement_analytics  |  Stage: silver -> gold (dimensions)
# Builds: bridge_item, dim_item, dim_site, dim_country, dim_currency, dim_date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

spark = SparkSession.builder.getOrCreate()

# --- bridge_item (item_no grain) ---
# = silver_mdb as-is. fact_movements / fact_item_site_setup / fact_stock join here on item_no.
bridge_item = spark.table("silver_mdb").select("MPN", "item_no", "company_id", "item_description")
(bridge_item.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_bridge_item"))
print(f"gold_bridge_item: {bridge_item.count()} rows")

# --- dim_item (company_id grain) ---
# Dedupe MDB to one row per company_id. MDB shows multiple inconsistent
# item_description spellings per company_id (e.g. VVX0000315 has 10 variants,
# all "O-RING 25X2,5NBR PN:0-162-20-121-2" spelled differently).
# Display-description rule: pick the longest description as the canonical one
# (longest tends to be the most complete/least abbreviated in this dataset) -
# flagged for stakeholder review at Gate #4 sign-off.
w = Window.partitionBy("company_id").orderBy(F.length("item_description").desc(), "item_no")

dim_item = (
    spark.table("silver_mdb")
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select(
        "company_id",
        F.col("MPN").alias("mpn"),
        F.col("item_description").alias("display_description"),
        F.lit(None).cast("int").alias("item_no_variant_count"),  # filled below
    )
)

variant_counts = (
    spark.table("silver_mdb").groupBy("company_id").agg(F.count("item_no").alias("item_no_variant_count"))
)
dim_item = (
    dim_item.drop("item_no_variant_count")
    .join(variant_counts, "company_id", "left")
)

(dim_item.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_dim_item"))
print(f"gold_dim_item: {dim_item.count()} rows (one per company_id)")

# sanity check for the O-ring test case
dim_item.filter("company_id = 'VVX0000315'").show(truncate=False)

# --- dim_site -> dim_country -> dim_currency chain (SI / CD / CX) ---
si = spark.table("silver_si")
cd = spark.table("silver_cd")

dim_site = (
    si.join(cd, "country_id", "left")
    .select(
        F.col("site"),
        F.col("site_name"),
        F.col("site_cur"),
        F.col("country_id"),
        F.col("country"),
    )
)
(dim_site.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_dim_site"))
print(f"gold_dim_site: {dim_site.count()} rows")

# dim_currency: distinct currencies + CX rate table (fm_cur -> to_cur).
# Normalisation target currency (to_cur) and field-parameter toggle are defined
# at semantic-model layer (Level 4) - see dax/measures.md.
dim_currency = (
    spark.table("silver_cx")
    .select("fm_cur", "to_cur", "rate")
    .distinct()
)
(dim_currency.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_dim_currency"))
print(f"gold_dim_currency: {dim_currency.count()} rows")

# --- dim_date ---
# Span: cover silver_mobo15 posting_date range (2020-07-01 to latest, observed
# 2024-08-23) plus current year, so Direct Lake relationships don't orphan rows.
date_range = spark.sql("""
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2026-12-31'), interval 1 day)) AS date
""")
dim_date = (
    date_range
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
)
(dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_dim_date"))
print(f"gold_dim_date: {dim_date.count()} rows")
# Mark as Date Table in the Direct Lake semantic model (Level 4) using gold_dim_date[date].


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
