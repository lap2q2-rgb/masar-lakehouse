from pyspark.sql import functions as F

from masar.config import GOLD, SILVER
from masar.spark import get_spark


def build_training_set(label_col: str = "duration_min"):
    spark = get_spark("training-join")
    feats = spark.read.format("delta").load(f"{GOLD}/trip_features")
    labels = (spark.read.format("delta").load(SILVER)
              .select("trip_id", F.col(label_col).alias("label")))
    return (feats.join(labels, "trip_id", "inner")
                 .withColumn("_trained_on_feature_version", F.col("_feature_version")))


if __name__ == "__main__":
    df = build_training_set()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/training_set")
    print(f"training rows: {df.count():,}")
    print("columns:", df.columns)
    df.show(5, truncate=False)
