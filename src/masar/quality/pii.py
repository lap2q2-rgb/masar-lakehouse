import argparse
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.spark import get_spark

RETENTION_DAYS = 180

PII_COLUMNS = {
    "trips": {
        "rider_id": "direct identifier: pseudonymise for BI",
        "driver_id": "direct identifier: pseudonymise for BI",
        "dropoff_geohash": "location: quasi-identifier, coarsen",
    },
    "gps_pings": {
        "driver_id": "direct identifier",
        "trip_id": "links a ping to a person's journey",
        "lat": "precise location",
        "lon": "precise location",
    },
}


def enforce_retention(path, days=RETENTION_DAYS, as_of=None, apply=False, purge=False):
    s = get_spark("pii")
    as_of = as_of or datetime.now()
    cutoff = as_of - timedelta(days=days)
    dt = DeltaTable.forPath(s, path)
    total = dt.toDF().count()
    old = dt.toDF().filter(F.col("event_ts").cast("timestamp") < F.lit(cutoff)).count()
    print(f"cutoff {cutoff:%Y-%m-%d}: {old:,} of {total:,} rows are past {days} days")
    if not apply:
        print("dry run: nothing deleted (add --apply)")
        return
    dt.delete(F.col("event_ts").cast("timestamp") < F.lit(cutoff))
    print(f"deleted. table now {dt.toDF().count():,} rows")
    if purge:
        s.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        dt.vacuum(0)
        print("vacuumed: old files physically removed")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--path", required=True)
    p.add_argument("--days", type=int, default=RETENTION_DAYS)
    p.add_argument("--as-of", help="YYYY-MM-DD HH:MM, default now")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--purge", action="store_true")
    a = p.parse_args()
    as_of = datetime.strptime(a.as_of, "%Y-%m-%d %H:%M") if a.as_of else None
    enforce_retention(a.path, a.days, as_of, a.apply, a.purge)