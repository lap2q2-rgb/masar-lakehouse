import pandas as pd
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark

spark = get_spark("break-it")


def count():
    return spark.read.format("delta").load(SILVER).count()


print("before:", count())
bad = spark.createDataFrame(pd.read_parquet("data/fixtures/dirty_batch_bad_fares.parquet"))
for f in spark.read.format("delta").load(SILVER).schema.fields:
    if f.name in bad.columns:
        bad = bad.withColumn(f.name, F.col(f.name).cast(f.dataType))
try:
    bad.write.format("delta").mode("append").save(SILVER)
    print("UNEXPECTED: write succeeded")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:300])
print("after bad fares:", count())

bad_type = spark.createDataFrame([("T-BAD-2", "Riyadh", "ten")], ["trip_id", "city", "fare_sar"])
try:
    bad_type.write.format("delta").mode("append").save(SILVER)
    print("UNEXPECTED: write succeeded")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:300])
print("after bad type:", count())
