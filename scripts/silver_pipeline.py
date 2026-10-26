"""
Silver layer pipeline.
Reads raw Airbnb CSV from /shared/data/bronze,
joins with census tract shapefiles (Apache Sedona) and ACS income CSV,
then writes the enriched parquet to /shared/data/silver/listings_enriched.
"""

import os
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ─────────────────────────────────────────
# 1. Paths
# ─────────────────────────────────────────
BRONZE_BASE = "/shared/data/bronze"
SILVER_BASE = "/shared/data/silver"

# ─────────────────────────────────────────
# 2. Spark session (with Apache Sedona)
# ─────────────────────────────────────────
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

SEDONA_PKG = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1"

spark = (
    SparkSession.builder
    .appName("airbnb-bronze2silver")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.jars.packages", SEDONA_PKG)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.broadcastTimeout", 3600)
    .config("spark.sql.shuffle.partitions", 200)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

from sedona.spark import SedonaContext  # noqa: E402 – must come after SparkSession
SedonaContext.create(spark)

# ─────────────────────────────────────────
# 3. Config
# ─────────────────────────────────────────
CITIES = ["los_angeles", "new_york_city", "portland", "san_francisco"]

CITY_SNAPSHOTS = {
    "los_angeles":    ["2025-03-01", "2025-06-17", "2025-09-01", "2025-12-04"],
    "new_york_city":  ["2025-03-01", "2025-06-17", "2025-09-01", "2025-12-04"],
    "portland":       ["2025-03-03", "2025-06-17", "2025-09-06", "2025-12-04"],
    "san_francisco":  ["2025-03-01", "2025-06-17", "2025-09-01", "2025-12-04"],
}

CITY_TO_TRACT_SHAPE_DIR = {
    "los_angeles":   f"{BRONZE_BASE}/city=los_angeles/shape_ca_2025",
    "san_francisco": f"{BRONZE_BASE}/city=los_angeles/shape_ca_2025",
    "new_york_city": f"{BRONZE_BASE}/city=new_york_city/shape_nyc_2025",
    "portland":      f"{BRONZE_BASE}/city=portland/shape_oregon_2025",
}

CITY_TO_INCOME_CSV = {
    "los_angeles":   f"{BRONZE_BASE}/city=los_angeles/income_la/ACSDT5Y2024.B19013-Data.csv",
    "san_francisco": f"{BRONZE_BASE}/city=san_francisco/income_sf/ACSDT5Y2024.B19013-Data.csv",
    "new_york_city": f"{BRONZE_BASE}/city=new_york_city/income_nyc/ACSDT5Y2024.B19013-Data.csv",
    "portland":      f"{BRONZE_BASE}/city=portland/income_oregon/ACSDT5Y2024.B19013-Data.csv",
}

# ─────────────────────────────────────────
# 4. Load & clean all raw listings
# ─────────────────────────────────────────
def _read_one(city: str, snapshot: str) -> DataFrame:
    path = f"{BRONZE_BASE}/city={city}/snapshot_date={snapshot}/listings.csv.gz"
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
        .withColumn("city", F.lit(city))
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot)))
    )


def _union_allow_missing(dfs: list) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


print("Reading bronze listings ...")
raw_dfs = [
    _read_one(city, snap)
    for city, snaps in CITY_SNAPSHOTS.items()
    for snap in snaps
]
airbnb_raw = _union_allow_missing(raw_dfs)

airbnb_listings = (
    airbnb_raw
    .withColumnRenamed("id", "listing_id")
    .withColumn("snapshot_date", F.to_date("snapshot_date"))
    .withColumn("latitude",  F.expr("try_cast(latitude  as double)"))
    .withColumn("longitude", F.expr("try_cast(longitude as double)"))
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .filter((F.col("latitude")  >= -90)  & (F.col("latitude")  <= 90))
    .filter((F.col("longitude") >= -180) & (F.col("longitude") <= 180))
    .withColumn(
        "neighbourhood_key",
        F.trim(F.lower(F.coalesce(
            F.col("neighbourhood_cleansed"),
            F.col("neighbourhood")
        )))
    )
)

# ─────────────────────────────────────────
# 5. Geospatial + income enrichment
# ─────────────────────────────────────────
def _load_acs(income_csv: str) -> DataFrame:
    return (
        spark.read.option("header", True).csv(income_csv)
        .filter(F.col("GEO_ID").startswith("1400000US"))
        .withColumn("tract_geoid",
            F.regexp_extract(F.col("GEO_ID"), r"US(\d{11})$", 1))
        .withColumn("median_income_str",
            F.regexp_replace(F.col("B19013_001E"), r"[^0-9]", ""))
        .withColumn("median_income",
            F.when(F.col("median_income_str") == "", F.lit(None))
             .otherwise(F.col("median_income_str").cast("double")))
        .select("tract_geoid", "median_income")
        .filter(F.length("tract_geoid") == 11)
    )


def enrich_city(city: str) -> DataFrame:
    city_df = airbnb_listings.filter(F.col("city") == city)

    # Load shapefile and keep only tracts within the city bounding box
    tracts = (
        spark.read.format("shapefile")
        .load(CITY_TO_TRACT_SHAPE_DIR[city])
        .select(
            F.col("GEOID").alias("tract_geoid"),
            F.col("geometry").alias("tract_geom"),
        )
    )

    bbox = city_df.agg(
        F.min("longitude").alias("minx"), F.max("longitude").alias("maxx"),
        F.min("latitude").alias("miny"),  F.max("latitude").alias("maxy"),
    ).collect()[0]
    bbox_wkt = (
        f"POLYGON(({bbox.minx} {bbox.miny}, {bbox.maxx} {bbox.miny}, "
        f"{bbox.maxx} {bbox.maxy}, {bbox.minx} {bbox.maxy}, "
        f"{bbox.minx} {bbox.miny}))"
    )
    tracts = tracts.filter(
        F.expr(f"ST_Intersects(tract_geom, ST_GeomFromWKT('{bbox_wkt}'))")
    )

    # Spatial join: assign each listing to its census tract
    city_geo = city_df.withColumn("geom", F.expr("ST_Point(longitude, latitude)"))
    joined = (
        city_geo
        .join(tracts, F.expr("ST_Contains(tract_geom, geom)"), "left")
        .drop("geom", "tract_geom")
    )

    # Join ACS median income by tract GEOID
    acs = _load_acs(CITY_TO_INCOME_CSV[city])
    return joined.join(acs, on="tract_geoid", how="left")


print("Enriching listings with tract geometry and ACS income ...")
enriched = _union_allow_missing([enrich_city(c) for c in CITIES])

# ─────────────────────────────────────────
# 6. Write silver
# ─────────────────────────────────────────
SILVER_PATH = f"{SILVER_BASE}/listings_enriched"
print(f"Writing silver layer to {SILVER_PATH} ...")

(
    enriched
    .write
    .mode("overwrite")
    .partitionBy("city", "snapshot_date")
    .parquet(SILVER_PATH)
)

print("Bronze to Silver complete.")
spark.stop()
