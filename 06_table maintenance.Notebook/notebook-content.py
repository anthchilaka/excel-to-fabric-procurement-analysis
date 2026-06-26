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

# Fabric Notebook: 06_table_maintenance
# Lakehouse: lh_procurement_analytics  |  Stage: gold maintenance (Amit Class 4-5)
# Run after each gold rebuild. Applies V-Order + OPTIMIZE, documents a
# DESCRIBE HISTORY checkpoint, and VACUUMs old versions per retention policy.

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# V-Order: enable for all subsequent Delta writes in this session (improves
# Direct Lake / Power BI read performance - Fabric-specific Parquet sort+encode).
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

GOLD_TABLES = [
    "gold_bridge_item",
    "gold_dim_item",
    "gold_dim_site",
    "gold_dim_currency",
    "gold_dim_date",
    "gold_fact_item_site_setup",
    "gold_fact_movements",
    "gold_fact_stock",
]

for tbl in GOLD_TABLES:
    spark.sql(f"OPTIMIZE {tbl}")
    print(f"OPTIMIZE {tbl} done")

# --- DESCRIBE HISTORY checkpoint ---
# Record the version number after each gold rebuild so RESTORE / time-travel
# (VERSION AS OF / TIMESTAMP AS OF) is available if a recompute introduces a
# regression (e.g. recommendation_flag logic change).
for tbl in GOLD_TABLES:
    history = spark.sql(f"DESCRIBE HISTORY {tbl} LIMIT 1").collect()
    if history:
        row = history[0]
        print(f"{tbl}: version={row['version']} timestamp={row['timestamp']} "
              f"operation={row['operation']}")

# --- VACUUM (retention: 30 days default) ---
# Run periodically, not every build - commented out to avoid accidental data loss
# during active development where time-travel to earlier gold versions is useful.
# for tbl in GOLD_TABLES:
#     spark.sql(f"VACUUM {tbl} RETAIN 720 HOURS")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

spark.createDataFrame([Row(id=1)]).write.format("delta").mode("overwrite").saveAsTable("measures_overview")
spark.createDataFrame([Row(id=1)]).write.format("delta").mode("overwrite").saveAsTable("measures_opportunity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT DISTINCT f.site, f.item_no
# MAGIC FROM gold_fact_item_site_setup f
# MAGIC INNER JOIN gold_bridge_item b
# MAGIC     ON f.item_no = b.item_no
# MAGIC WHERE b.company_id = 'VVX0000315'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
