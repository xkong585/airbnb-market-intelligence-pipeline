from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# --------------------------------
# 1. Paths
# --------------------------------
SILVER_BASE = "/shared/data/silver"
GOLDEN_BASE = "/shared/data/golden"

# --------------------------------
# 2. Create Spark Session
# --------------------------------
spark = SparkSession.builder \
    .appName("Airbnb Gold Layer Pipeline") \
    .getOrCreate()

# --------------------------------
# 3. Load Silver Layer
# --------------------------------
df = spark.read.parquet(f"{SILVER_BASE}/listings_enriched")

# --------------------------------
# 4. Create Time Features
# --------------------------------
df = df.withColumn("year", year("snapshot_date")) \
       .withColumn("month", month("snapshot_date"))

# =================================
# GOLD TABLE 1
# fact_neighborhood_month
# =================================

fact_neighborhood_month = (
    df.groupBy(
        "city",
        "neighbourhood_cleansed",
        "year",
        "month"
    )
    .agg(
        count_distinct("listing_id").alias("listing_count"),
        sum("number_of_reviews").alias("review_count"),
        avg("reviews_per_month").alias("avg_reviews_per_month"),
        avg("availability_365").alias("avg_availability_365"),
        avg("estimated_occupancy_l365d").alias("avg_estimated_occupancy"),
        avg("median_income").alias("median_income")
    )
)

fact_neighborhood_month.write \
    .mode("overwrite") \
    .partitionBy("city", "year") \
    .parquet(f"{GOLDEN_BASE}/fact_neighborhood_month")

# =================================
# GOLD TABLE 2
# fact_host_summary
# =================================

# Each host portfolio is specific to city, snapshot and neighborhood.
host_keys = ["city", "snapshot_date", "neighbourhood_cleansed"]
host_listing_counts = (
    df.filter(col("host_id").isNotNull())
    .groupBy(*host_keys, "host_id")
    .agg(count_distinct("listing_id").alias("host_listing_count"))
)
host_rollup = host_listing_counts.groupBy(*host_keys).agg(
    count_distinct("host_id").alias("host_count"),
    sum("host_listing_count").alias("known_host_listing_count"),
    avg("host_listing_count").alias("avg_listings_per_host"),
    max("host_listing_count").alias("largest_host_size"),
)
all_listings = df.groupBy(*host_keys).agg(count_distinct("listing_id").alias("total_listings"))
fact_host_summary = all_listings.join(host_rollup, host_keys, "left").withColumn(
    "largest_host_share_known",
    when(col("known_host_listing_count") > 0,
         col("largest_host_size") / col("known_host_listing_count"))
)

fact_host_summary.write \
    .mode("overwrite") \
    .parquet(f"{GOLDEN_BASE}/fact_host_summary")

# =================================
# GOLD TABLE 3
# fact_listing_summary
# =================================

fact_listing_summary = (
    df.select(
        "listing_id",
        "snapshot_date",
        "city",
        "neighbourhood_cleansed",
        "year",
        "month",
        "room_type",
        "accommodates",
        "number_of_reviews",
        "reviews_per_month",
        "availability_365",
        "estimated_occupancy_l365d",
        "estimated_revenue_l365d"
    )
)

fact_listing_summary.write \
    .mode("overwrite") \
    .partitionBy("city", "year") \
    .parquet(f"{GOLDEN_BASE}/fact_listing_summary")

# --------------------------------
# 5. Preview Tables
# --------------------------------
print("fact_neighborhood_month preview:")
spark.read.parquet(f"{GOLDEN_BASE}/fact_neighborhood_month").show(5)

print("fact_host_summary preview:")
spark.read.parquet(f"{GOLDEN_BASE}/fact_host_summary").show(5)

print("fact_listing_summary preview:")
spark.read.parquet(f"{GOLDEN_BASE}/fact_listing_summary").show(5)

print("Gold Layer tables successfully created.")

spark.stop()
