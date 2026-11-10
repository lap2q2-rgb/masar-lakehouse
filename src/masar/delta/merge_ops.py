import json
import sys
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark


def _version(spark) -> int:
    return DeltaTable.forPath(spark, SILVER).history(1).select("version").first()[0]


def upsert_corrections(spark, path: str) -> None:
    """MERGE keyed on trip_id. Only rows whose fare really changes are updated."""
    src = (spark.read.option("header", True).csv(path)
           .select("trip_id", F.col("fare_sar").cast("double").alias("fare_sar")))
    before = _version(spark)
    (DeltaTable.forPath(spark, SILVER).alias("t")
        .merge(src.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdate(condition="t.fare_sar <> s.fare_sar",
                           set={"fare_sar": "s.fare_sar"})
        .execute())
    after = _version(spark)
    if after == before:
        print(f"no changes: nothing committed (still v{after})")
    else:
        m = (DeltaTable.forPath(spark, SILVER).history(1)
             .select(F.col("operationMetrics")["numTargetRowsUpdated"].alias("n")).first())
        print(f"MERGE committed as v{after}: rows_updated={m['n']}")


def show_fares(spark, path: str) -> None:
    ids = [r.trip_id for r in spark.read.option("header", True).csv(path).collect()]
    (spark.read.format("delta").load(SILVER)
        .where(F.col("trip_id").isin(ids))
        .select("trip_id", "fare_sar").orderBy("trip_id").show())


def erase_rider(spark, rider_id: str) -> None:
    """PDPL erasure: one atomic, logged commit."""
    silver = DeltaTable.forPath(spark, SILVER)
    before = spark.read.format("delta").load(SILVER).count()
    silver.delete(F.col("rider_id") == rider_id)
    m = (silver.history(1)
         .select("version", F.col("operationMetrics")["numDeletedRows"].alias("deleted"))
         .first())
    after = spark.read.format("delta").load(SILVER).count()
    print(f"erased {rider_id}: v{m['version']} deleted={m['deleted']}  rows {before} -> {after}")


if __name__ == "__main__":
    spark = get_spark("merge-ops")
    csv = "data/fixtures/corrections.csv"
    if sys.argv[1] == "corrections":
        show_fares(spark, csv)
        upsert_corrections(spark, csv)
        show_fares(spark, csv)
    elif sys.argv[1] == "erase":
        rid = json.load(open("data/fixtures/erasure_request.json"))["rider_id"]
        erase_rider(spark, rid)
