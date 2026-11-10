from pyspark.sql import functions as F
from masar.config import LAKEHOUSE, SILVER
from masar.spark import get_spark

spark = get_spark("evolve")
DEMO = str(LAKEHOUSE / "scratch" / "evolve_demo")

base = spark.read.format("delta").load(SILVER).limit(5)
base.write.format("delta").mode("overwrite").save(DEMO)
new = base.withColumn("promo_code", F.lit("RAMADAN10"))

try:
    new.write.format("delta").mode("append").save(DEMO)
    print("UNEXPECTED: write succeeded")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:200])

new.write.format("delta").mode("append").option("mergeSchema", "true").save(DEMO)
spark.read.format("delta").load(DEMO).select("trip_id", "promo_code").show()
