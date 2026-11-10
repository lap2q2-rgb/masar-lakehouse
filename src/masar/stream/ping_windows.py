from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark

BRONZE = str(LAKEHOUSE / "bronze" / "gps_stream")
GOLD = str(LAKEHOUSE / "gold" / "ping_windows_5m")
CHECKPOINT = str(LAKEHOUSE / "_checkpoints" / "ping_windows")

spark = get_spark("ping-windows")
pings = spark.readStream.format("delta").load(BRONZE)

by_window = (pings
    .withWatermark("event_ts", "3 minutes")
    .groupBy(F.window("event_ts", "5 minutes"), F.col("city"))
    .agg(F.count("*").alias("pings")))

(by_window.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # process what exists, then stop
    .start(GOLD)
    .awaitTermination())

(spark.read.format("delta").load(GOLD)
    .orderBy("window", "city").show(10, truncate=False))
