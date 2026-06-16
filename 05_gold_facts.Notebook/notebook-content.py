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
    .withColumn("Value_Unrestricted_GBP", F.col("Value_Unrestricted") * F.col("rate_to_gbp"))
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
