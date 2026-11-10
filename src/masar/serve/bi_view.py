import os

from pyspark.sql import functions as F

from masar.config import GOLD, SILVER
from masar.quality.pii import PII_COLUMNS
from masar.spark import get_spark

SALT = os.environ.get("MASAR_SALT", "masar-workshop-salt")
PSEUDONYMISE = ("rider_id", "driver_id")
COARSEN = {"dropoff_geohash": 4}   # keep only the first 4 characters


def build_bi_safe_view() -> None:
    handled = set(PSEUDONYMISE) | set(COARSEN)
    unhandled = set(PII_COLUMNS["trips"]) - handled
    assert not unhandled, f"PII columns with no BI treatment: {sorted(unhandled)}"

    spark = get_spark("bi-view")
    silver = spark.read.format("delta").load(SILVER)
    safe = silver
    for col in PSEUDONYMISE:
        safe = safe.withColumn(col.replace("_id", "_pseudonym"),
                               F.sha2(F.concat(F.lit(SALT), F.col(col)), 256))
    for col, n in COARSEN.items():
        safe = safe.withColumn(col.replace("_geohash", "_area"), F.substring(col, 1, n))
    safe = safe.select("trip_id", "city", "rider_pseudonym", "driver_pseudonym",
                       "dropoff_area", "fare_sar", "distance_km", "duration_min", "started_at")
    safe.write.format("delta").mode("overwrite").save(f"{GOLD}/bi_trips_safe")
    print(f"bi_trips_safe rows: {safe.count():,}")
    print("columns:", safe.columns)


if __name__ == "__main__":
    build_bi_safe_view()
