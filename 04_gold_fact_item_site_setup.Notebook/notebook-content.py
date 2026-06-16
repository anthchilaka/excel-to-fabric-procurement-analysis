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

# Fabric Notebook: 04_gold_fact_item_site_setup
# Lakehouse: lh_procurement_analytics  |  Stage: silver -> gold (fact_item_site_setup)
#
# VALIDATED against the O-ring test case (company_id VVX0000315, 14-row expected
# result, gold_layer_build_spec.md Section 9) - see logs/oring_validation.py for
# the pandas prototype that produced an exact 14/14 match.
#
# Per Level-4 constraint (Direct Lake): recommendation_flag and movement_status
# must be PHYSICAL columns in this gold table, not DAX calculated columns.
#
# *** OPEN ITEM - movement_status recompute (see troubleshooting_log.md) ***
# gold_layer_build_spec.md specifies recomputing movement_status from
# fact_movements (MOBO15) using a 36-month rolling window + an undisclosed
# `calculated_maximum` formula. Investigation this session found:
#   - For the O-ring test case item_no/site pairs, MOBO15 transaction history
#     is a single posting batch per item/site (not a time series spread across
#     36 months in this sample export), so average_issue_6/12 and
#     average_quantity_6/12 cannot be derived empirically from this sample.
#   - calculated_maximum remains undisclosed and unvalidated.
# DECISION (this session): retain silver_mm.movement_status as the gold
# movement_status source for now (MM is itself described as "a Python-calculated
# output derived from MOBO15" per Meta_Data_KPI.md / data_dictionary log - i.e.
# the recompute already happened upstream, we just don't have the formula).
# The 36-month recompute notebook cell below is stubbed and DISABLED
# (RECOMPUTE_MOVEMENT_STATUS = False) until the formula is obtained from the
# stakeholder or derived from a fuller transaction history export.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

RECOMPUTE_MOVEMENT_STATUS = False  # see OPEN ITEM note above

ms = spark.table("silver_ms")
mm = spark.table("silver_mm")
bridge_item = spark.table("gold_bridge_item")  # item_no -> company_id

if RECOMPUTE_MOVEMENT_STATUS:
    # --- STUB: 36-month rolling window recompute from fact_movements ---
    # mobo15 = spark.table("silver_mobo15")
    # as_of = mobo15.agg(F.max("posting_date")).first()[0]
    # window_start = F.add_months(F.lit(as_of), -36)
    # movements_36m = mobo15.filter(
    #     (F.col("posting_date") >= window_start) &
    #     (F.col("_is_unparsed_date") == False) &
    #     (F.col("mt").isin("411", "471", "761", "765"))
    #     # 411 = Goods Issued cost centre, 471 = Goods Issued order,
    #     # 761/765 = scrapping (consumed/destroyed)
    #     # EXCLUDED: 311/312 (receipts/reversals), 511/519 (transfers),
    #     # 762 (reversal scrapping), 851 (transit), 911/912 (phys. inv. adjustments)
    #     # Rationale: only consumption events prove an item is active;
    #     # receipts, transfers and adjustments do not.
    # )
    # ... derive average_issue_6/12, average_quantity_6/12, maximum_quantity,
    #     calculated_maximum (FORMULA TBD), movement_status (Fast/Medium/Slow/No Mover)
    raise NotImplementedError("36-month recompute formula not yet validated - see OPEN ITEM note")
else:
    movement_status_src = mm.select(
        "item_no", "site",
        F.col("movement_status").alias("movement_status_src")
    )

# --- Full outer join: MS (item_status/setup) <-> movement_status source, on item_no+site ---
joined = (
    ms.alias("ms")
    .join(movement_status_src.alias("mm"), on=["item_no", "site"], how="full_outer")
)

# has_ms_record: distinguishes "no MS row at all" (Missing item setup) from
# "MS row exists but item_status is blank" (No flag / normal active item)
fact_item_site_setup = (
    joined
    .withColumn("has_ms_record", F.col("ms.minimum_level").isNotNull() |
                                    F.col("deletion_indicator").isNotNull())
    .withColumn(
        "movement_status",
        F.when(F.col("movement_status_src").isNotNull(), F.col("movement_status_src"))
         .when(F.col("has_ms_record"), F.lit("Non-moving"))   # MS exists, no MM record -> Non-moving
         .otherwise(F.lit(None))
    )
    .withColumn(
        "recommendation_flag",
        F.when(
            (F.col("deletion_indicator") == "X") &
            (F.col("movement_status").isin("Fast", "Medium", "Slow")),
            F.lit("Review before deletion — still active")
        ).when(
            (F.col("deletion_indicator") == "X") &
            (F.col("movement_status").isNull() | (F.col("movement_status") == "Non-moving")),
            F.lit("Deletion confirmed — no activity")
        ).when(
            (~F.col("has_ms_record")) & F.col("movement_status").isNotNull(),
            F.lit("Missing item setup — add min/max/reorder")
        ).when(
            F.col("has_ms_record") & F.col("movement_status_src").isNull() &
            (F.col("movement_status") == "Non-moving"),
            F.lit("Confirmed idle — opportunity candidate")
        ).otherwise(F.lit("No flag"))
    )
    .join(bridge_item.select("item_no", "company_id"), "item_no", "left")
    .select(
        "item_no", "company_id", "site",
        "minimum_level", "reorder_point", "fixed_lot", "maximum_level",
        "deletion_indicator", "item_status", "mrp_type", "lot_size",
        "movement_status", "recommendation_flag",
    )
)

(fact_item_site_setup.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold_fact_item_site_setup"))
print(f"gold_fact_item_site_setup: {fact_item_site_setup.count()} rows")

# --- Validation: O-ring test case (company_id VVX0000315) ---
oring = (
    spark.table("gold_fact_item_site_setup")
    .filter("company_id = 'VVX0000315'")
    .orderBy("item_no", "site")
)
oring.show(20, truncate=False)
oring_count = oring.count()
assert oring_count == 14, f"O-ring test case expected 14 rows, got {oring_count}"
print("O-ring test case: PASS (14/14 rows)")

# --- Diagnostic: unparsed-date exposure vs movement_status (Data Governance) ---
# For every item_no x site, shows what % of MOBO15 rows had no parsable posting_date,
# alongside our assigned movement_status vs the raw MM value.
# Purpose: detect items classified as idle/non-moving purely due to missing dates,
# not genuine inactivity. Key integrity check for the opportunity table.

mobo15_diag = spark.table("silver_mobo15")

unparsed_summary = (
    mobo15_diag
    .groupBy("item_no", "site")
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(F.col("_is_unparsed_date").cast("int")).alias("unparsed_rows")
    )
    .withColumn("pct_unparsed_dates",
        F.round((F.col("unparsed_rows") / F.col("total_rows")) * 100, 1))
)

mm_ref = spark.table("silver_mm").select(
    "item_no", "site",
    F.col("movement_status").alias("mm_movement_status")
)

diag = (
    unparsed_summary
    .join(
        spark.table("gold_fact_item_site_setup")
            .select("item_no", "site", F.col("movement_status").alias("our_movement_status")),
        on=["item_no", "site"], how="left"
    )
    .join(mm_ref, on=["item_no", "site"], how="left")
    .withColumn("match",
        F.when(F.col("our_movement_status") == F.col("mm_movement_status"), "YES")
         .otherwise("NO"))
    .select("item_no", "site", "pct_unparsed_dates",
            "our_movement_status", "mm_movement_status", "match")
    .orderBy(F.col("pct_unparsed_dates").desc())
)

print("\n--- Unparsed-date exposure diagnostic (top 20 by % unparsed) ---")
diag.show(20, truncate=False)

mismatches = diag.filter("match = 'NO'").count()
high_exposure = diag.filter("pct_unparsed_dates >= 50").count()
print(f"movement_status mismatches (our vs MM): {mismatches}")
print(f"item-site pairs with >=50% unparsed dates: {high_exposure}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
