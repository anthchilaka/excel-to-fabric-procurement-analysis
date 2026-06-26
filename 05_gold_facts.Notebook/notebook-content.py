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

# Fabric Notebook: 05_gold_facts
# Lakehouse: lh_procurement_analytics  |  Stage: silver -> gold (fact tables)
# Builds: fact_movements (MOBO15), fact_stock (MOBO25) with currency normalisation.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# --- fact_movements (transaction grain, confirmed no change needed) ---
fact_movements = (
    spark.table("silver_mobo15")
    .select(
        "posting_date", "site", "mt", "mt_text", "item_document", "item_no",
        "item_description", "quantity", "base_uom", "user_name",
        "storage_location", "Purchase_order", "_is_unparsed_date",
    )
)
(fact_movements.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("site")
    .saveAsTable("gold_fact_movements"))
print(f"gold_fact_movements: {fact_movements.count()} rows")

# --- fact_stock (current stock snapshot, MOBO25) ---
# Currency normalisation applied here at gold (native + normalized both retained,
# per gold_layer_build_spec.md Section 5) - normalized to GBP as the reporting
# default; field parameter at semantic layer lets users toggle native vs normalized.
mobo25 = spark.table("silver_mobo25")
dim_site = spark.table("gold_dim_site")
cx = spark.table("silver_cx")

stock_with_site = mobo25.join(dim_site.select("site", "site_cur"), "site", "left")

# rate: site_cur -> GBP. CX is fm_cur -> to_cur; filter to to_cur = 'GBP'.
rate_to_gbp = cx.filter("to_cur = 'GBP'").select(
    F.col("fm_cur").alias("site_cur"), "rate"
)

fact_stock = (
    stock_with_site
    .join(rate_to_gbp, "site_cur", "left")
    .withColumn(
        "rate_to_gbp",
        F.when(F.col("site_cur") == "GBP", F.lit(1.0)).otherwise(F.col("rate"))
    )
    .withColumn("Value_Unrestricted_GBP", F.col("Value_Unrestricted") / F.col("rate_to_gbp"))
    # DIVIDE not multiply: CX rate = "X units of site_cur per 1 GBP" (SAP/ERP convention).
    # Bug found 2026-06-17 during KPI validation — multiply inflated African-currency sites
    # (NGN, KES, TZS, UGX) by factors of millions, causing £142T total stock value.
    # Fix: divide converts site_cur amount → GBP correctly.
    .drop("rate")
)
(fact_stock.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_fact_stock"))
print(f"gold_fact_stock: {fact_stock.count()} rows")

# Unmapped currencies (no CX rate to GBP found) - flag for assumptions log follow-up.
unmapped = fact_stock.filter("rate_to_gbp IS NULL AND site_cur != 'GBP'")
n_unmapped = unmapped.count()
if n_unmapped:
    print(f"WARNING: {n_unmapped} fact_stock rows have no CX rate to GBP "
          f"(site_cur values: "
          f"{[r['site_cur'] for r in unmapped.select('site_cur').distinct().collect()]})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric Notebook: 05_gold_facts
# Lakehouse: lh_procurement_analytics  |  Stage: silver -> gold (fact tables)
# Builds: fact_movements (MOBO15), fact_stock (MOBO25) with currency normalisation.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# --- fact_movements (transaction grain, confirmed no change needed) ---
fact_movements = (
    spark.table("silver_mobo15")
    .select(
        "posting_date", "site", "mt", "mt_text", "item_document", "item_no",
        "item_description", "quantity", "base_uom", "user_name",
        "storage_location", "Purchase_order", "_is_unparsed_date",
    )
)
(fact_movements.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("site")
    .saveAsTable("gold_fact_movements"))
print(f"gold_fact_movements: {fact_movements.count()} rows")

# --- fact_stock (current stock snapshot, MOBO25) ---
# Currency normalisation applied here at gold (native + normalized both retained,
# per gold_layer_build_spec.md Section 5) - normalized to GBP as the reporting
# default; field parameter at semantic layer lets users toggle native vs normalized.
mobo25 = spark.table("silver_mobo25")
dim_site = spark.table("gold_dim_site")
cx = spark.table("silver_cx")

stock_with_site = mobo25.join(dim_site.select("site", "site_cur"), "site", "left")

# rate: site_cur -> GBP. CX is fm_cur -> to_cur; filter to to_cur = 'GBP'.
rate_to_gbp = cx.filter("to_cur = 'GBP'").select(
    F.col("fm_cur").alias("site_cur"), "rate"
)

fact_stock = (
    stock_with_site
    .join(rate_to_gbp, "site_cur", "left")
    .withColumn(
        "rate_to_gbp",
        F.when(F.col("site_cur") == "GBP", F.lit(1.0)).otherwise(F.col("rate"))
    )
    .withColumn("Value_Unrestricted_GBP", F.col("Value_Unrestricted") / F.col("rate_to_gbp"))
    # DIVIDE not multiply: CX rate = "X units of site_cur per 1 GBP" (SAP/ERP convention).
    # Bug found 2026-06-17 during KPI validation — multiply inflated African-currency sites
    # (NGN, KES, TZS, UGX) by factors of millions, causing £142T total stock value.
    # Fix: divide converts site_cur amount → GBP correctly.
    .drop("rate")
)

# --- Join maximum_level from gold_fact_item_site_setup into fact_stock ---
# Required for Excess Inventory Rate DAX measure.
# NATURALINNERJOIN across gold_fact_stock + gold_fact_item_site_setup in Direct Lake
# causes rsQueryTimeoutExceeded (225s) on 556k x 129k rows. Fix: pre-join at gold
# build stage so DAX runs as single-table SUMX. (Bug logged 2026-06-23.)
df_setup = spark.table("gold_fact_item_site_setup") \
    .select("item_no", "site", "maximum_level")

fact_stock = fact_stock.join(df_setup, on=["item_no", "site"], how="left")

(fact_stock.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_fact_stock"))
print(f"gold_fact_stock: {fact_stock.count()} rows")

# Unmapped currencies (no CX rate to GBP found) - flag for assumptions log follow-up.
unmapped = fact_stock.filter("rate_to_gbp IS NULL AND site_cur != 'GBP'")
n_unmapped = unmapped.count()
if n_unmapped:
    print(f"WARNING: {n_unmapped} fact_stock rows have no CX rate to GBP "
          f"(site_cur values: "
          f"{[r['site_cur'] for r in unmapped.select('site_cur').distinct().collect()]})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("gold_fact_stock")
print(df.columns)
print(df.select("maximum_level").limit(5).show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric Notebook: 05_gold_facts
# Lakehouse: lh_procurement_analytics  |  Stage: silver -> gold (fact tables)
# Builds: fact_movements (MOBO15), fact_stock (MOBO25) with currency normalisation.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# --- fact_movements (transaction grain, confirmed no change needed) ---
fact_movements = (
    spark.table("silver_mobo15")
    .select(
        "posting_date", "site", "mt", "mt_text", "item_document", "item_no",
        "item_description", "quantity", "base_uom", "user_name",
        "storage_location", "Purchase_order", "_is_unparsed_date",
    )
)
(fact_movements.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("site")
    .saveAsTable("gold_fact_movements"))
print(f"gold_fact_movements: {fact_movements.count()} rows")

# --- fact_stock (current stock snapshot, MOBO25) ---
# Currency normalisation applied here at gold (native + normalized both retained,
# per gold_layer_build_spec.md Section 5) - normalized to GBP as the reporting
# default; field parameter at semantic layer lets users toggle native vs normalized.
mobo25 = spark.table("silver_mobo25")
dim_site = spark.table("gold_dim_site")
cx = spark.table("silver_cx")

stock_with_site = mobo25.join(dim_site.select("site", "site_cur"), "site", "left")

# rate: site_cur -> GBP. CX is fm_cur -> to_cur; filter to to_cur = 'GBP'.
rate_to_gbp = cx.filter("to_cur = 'GBP'").select(
    F.col("fm_cur").alias("site_cur"), "rate"
)

fact_stock = (
    stock_with_site
    .join(rate_to_gbp, "site_cur", "left")
    .withColumn(
        "rate_to_gbp",
        F.when(F.col("site_cur") == "GBP", F.lit(1.0)).otherwise(F.col("rate"))
    )
    .withColumn("Value_Unrestricted_GBP", F.col("Value_Unrestricted") / F.col("rate_to_gbp"))
    # DIVIDE not multiply: CX rate = "X units of site_cur per 1 GBP" (SAP/ERP convention).
    # Bug found 2026-06-17 during KPI validation — multiply inflated African-currency sites
    # (NGN, KES, TZS, UGX) by factors of millions, causing £142T total stock value.
    # Fix: divide converts site_cur amount -> GBP correctly.
    .drop("rate")
)

# --- Join maximum_level from gold_fact_item_site_setup into fact_stock ---
# Required for Excess Inventory Rate DAX measure.
# NATURALINNERJOIN across gold_fact_stock + gold_fact_item_site_setup in Direct Lake
# causes rsQueryTimeoutExceeded (225s) on 556k x 129k rows. Fix: pre-join at gold
# build stage so DAX runs as single-table SUMX. (Bug logged 2026-06-23.)
df_setup = spark.table("gold_fact_item_site_setup") \
    .select("item_no", "site", "maximum_level")

fact_stock = fact_stock.join(df_setup, on=["item_no", "site"], how="left")

(fact_stock.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_fact_stock"))
print(f"gold_fact_stock: {fact_stock.count()} rows")

# Unmapped currencies (no CX rate to GBP found) - flag for assumptions log follow-up.
unmapped = fact_stock.filter("rate_to_gbp IS NULL AND site_cur != 'GBP'")
n_unmapped = unmapped.count()
if n_unmapped:
    print(f"WARNING: {n_unmapped} fact_stock rows have no CX rate to GBP "
          f"(site_cur values: "
          f"{[r['site_cur'] for r in unmapped.select('site_cur').distinct().collect()]})")

# --- Backfill total_value_unrestricted_gbp into gold_fact_item_site_setup ---
# Aggregates GBP-normalised stock value from gold_fact_stock at item_no x site grain,
# then merges back as a physical column on gold_fact_item_site_setup.
# Pre-computed here (not as a DAX measure) due to Direct Lake cross-table row-grain
# timeout — same architectural pattern as is_excess (Session 13).
stock_value = (
    spark.table("gold_fact_stock")
    .groupBy("item_no", "site")
    .agg(F.sum("Value_Unrestricted_GBP").alias("total_value_unrestricted_gbp"))
)

fact_setup = spark.table("gold_fact_item_site_setup")

fact_setup_updated = (
    fact_setup
    .join(stock_value, on=["item_no", "site"], how="left")
)

(fact_setup_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_fact_item_site_setup"))
print(f"gold_fact_item_site_setup with total_value_unrestricted_gbp: {fact_setup_updated.count()} rows")

spark.table("gold_fact_item_site_setup") \
    .filter("total_value_unrestricted_gbp IS NOT NULL") \
    .select("item_no", "site", "movement_status", "total_value_unrestricted_gbp") \
    .orderBy(F.col("total_value_unrestricted_gbp").desc()) \
    .show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
