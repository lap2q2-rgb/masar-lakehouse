import argparse
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark


def _read(path: str):
    return get_spark("observe").read.format("delta").load(path)


def check_freshness(path: str, sla_hours: int = 6) -> None:
    last = _read(path).agg(F.max("_ingested_at").alias("m")).collect()[0]["m"]
    if last is None or last < datetime.now() - timedelta(hours=sla_hours):
        raise RuntimeError(f"FRESHNESS breach: {path} last updated {last}")
    print(f"freshness ok: last updated {last}")


def check_volume(path: str, expected: int, tol: float = 0.3) -> None:
    n = _read(path).count()
    if abs(n - expected) / expected > tol:
        raise RuntimeError(f"VOLUME anomaly: {n:,} vs expected ~{expected:,}")
    print(f"volume ok: {n:,} rows (expected ~{expected:,})")


def check_fare_drift(path: str, baseline_mean: float, tol: float = 0.5
                     ) -> None:
    m = _read(path).agg(F.mean("fare_sar").alias("m")).collect()[0]["m"]
    if abs(m - baseline_mean) / baseline_mean > tol:
        print(f"[DRIFT] fare mean {m:.1f} vs baseline {baseline_mean:.1f}")
    else:
        print(f"drift ok: fare mean {m:.1f} vs baseline {baseline_mean:.1f}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--check", required=True, choices=["freshness", "volume", "drift"])
    p.add_argument("--path", default=SILVER)
    p.add_argument("--sla-hours", type=int, default=6)
    p.add_argument("--expected", type=int, default=44371)
    p.add_argument("--baseline", type=float, default=60.2)
    a = p.parse_args()
    if a.check == "freshness":
        check_freshness(a.path, a.sla_hours)
    elif a.check == "volume":
        check_volume(a.path, a.expected)
    else:
        check_fare_drift(a.path, a.baseline)