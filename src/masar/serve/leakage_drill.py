import numpy as np
from pyspark.sql import functions as F

from masar.config import GOLD, SILVER
from masar.spark import get_spark


def r2(X, y, seed=7):
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(y))
    cut = int(len(y) * 0.8)
    tr, te = idx[:cut], idx[cut:]
    A = np.c_[np.ones(len(y)), X]
    beta, *_ = np.linalg.lstsq(A[tr], y[tr], rcond=None)
    pred = A[te] @ beta
    return 1 - ((y[te] - pred) ** 2).sum() / ((y[te] - y[te].mean()) ** 2).sum()


def main():
    spark = get_spark("leakage")
    silver = spark.read.format("delta").load(SILVER)
    feats = spark.read.format("delta").load(f"{GOLD}/trip_features")
    hour = F.date_trunc("hour", F.col("started_at"))
    same_hour = (silver.withColumn("h", hour).groupBy("city", "h").count()
                 .withColumnRenamed("count", "demand_same_hour"))
    df = (feats.join(silver.select("trip_id", "duration_min", "fare_per_km"), "trip_id")
               .withColumn("h", hour)
               .join(same_hour, ["city", "h"])
               .select("duration_min", "start_hour", "is_night", "surge_multiplier",
                       "zone_demand_prev_hr", "fare_per_km", "demand_same_hour")
               .toPandas().dropna())
    y = df["duration_min"].to_numpy(float)
    base = ["start_hour", "is_night", "surge_multiplier", "zone_demand_prev_hr"]
    sets = {
        "A  prior-hour features only": base,
        "B  + fare_per_km": base + ["fare_per_km"],
        "C  + same-hour demand": base + ["demand_same_hour"],
    }
    print(f"rows: {len(df):,}   label: duration_min   (held-out R2)")
    for name, cols in sets.items():
        print(f"  R2 {r2(df[cols].to_numpy(float), y):6.3f}   {name}")


if __name__ == "__main__":
    main()
