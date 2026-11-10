from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from masar.config import GOLD, SILVER
from masar.spark import get_spark

RECENT_DAYS = 3


def build_zone_hourly_demand() -> None:
    """Grain: one row per (city, date, hour)."""
    spark = get_spark("gold-demand")
    silver = spark.read.format("delta").load(SILVER)
    demand = (silver
        .withColumn("date", F.to_date("started_at"))
        .withColumn("hour", F.hour("started_at"))
        .groupBy("city", "date", "hour")
        .agg(F.count("*").alias("trips"),
             F.avg("fare_sar").alias("avg_fare_sar"),
             F.avg("duration_min").alias("avg_duration_min"),
             F.avg("surge_multiplier").alias("avg_surge")))

    target = f"{GOLD}/zone_hourly_demand"
    writer = demand.write.format("delta").partitionBy("date").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, target):
        newest = demand.agg(F.max("date")).first()[0]
        since = newest - timedelta(days=RECENT_DAYS)
        (demand.filter(F.col("date") >= F.lit(since)).write.format("delta")
            .partitionBy("date").mode("overwrite")
            .option("replaceWhere", f"date >= '{since}'")
            .save(target))
    else:
        writer.save(target)
    print(f"  zone_hourly_demand rows: {spark.read.format('delta').load(target).count():,}")


def build_trip_features() -> None:
    """Grain: one row per trip. Point-in-time: only the prior hour is used."""
    spark = get_spark("gold-features")
    silver = spark.read.format("delta").load(SILVER)
    w = (Window.partitionBy("city")
         .orderBy(F.col("started_at").cast("long"))
         .rangeBetween(-3600, -1))
    features = (silver
        .withColumn("zone_demand_prev_hr", F.count("*").over(w))
        .select("trip_id", "city", "started_at",
                "start_hour", "is_night",
                "surge_multiplier", "zone_demand_prev_hr")
        .withColumn("_feature_version", F.lit("v1")))
    target = f"{GOLD}/trip_features"
    features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
    print(f"  trip_features rows: {spark.read.format('delta').load(target).count():,}")


def optimize_and_vacuum() -> None:
    spark = get_spark("gold-maintain")
    for name in ("zone_hourly_demand", "trip_features"):
        dt = DeltaTable.forPath(spark, f"{GOLD}/{name}")
        dt.optimize().executeCompaction()
        dt.vacuum()
        print(f"  maintained {name}")
