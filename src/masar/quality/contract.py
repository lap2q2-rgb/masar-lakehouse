import sys
import great_expectations as gx
import pandas as pd
from masar.config import SILVER
from masar.spark import get_spark

CITIES = ["Riyadh", "Jeddah", "Dammam", "Mecca", "Medina"]
INTEGRITY = ("trip_id_not_null", "trip_id_unique")      # a failure here blocks the whole batch

CHECKS = {
    "trip_id_not_null": lambda v: v.expect_column_values_to_not_be_null("trip_id"),
    "trip_id_unique":   lambda v: v.expect_column_values_to_be_unique("trip_id"),
    "fare_is_double":   lambda v: v.expect_column_values_to_be_of_type("fare_sar", "DoubleType"),
    "fare_positive":    lambda v: v.expect_column_values_to_be_between("fare_sar", min_value=0, strict_min=True),
    "city_known":       lambda v: v.expect_column_values_to_be_in_set("city", CITIES),
    "duration_sane":    lambda v: v.expect_column_values_to_be_between("duration_min", min_value=0, max_value=600),
    "fare_mean_sane":   lambda v: v.expect_column_mean_to_be_between("fare_sar", min_value=15, max_value=120),
}


def validate(df) -> dict:
    context = gx.get_context()
    source = context.sources.add_spark("masar_spark")
    asset = source.add_dataframe_asset("trips_batch")
    context.add_or_update_expectation_suite("silver_trips")
    validator = context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        expectation_suite_name="silver_trips")
    out = {}
    for name, check in CHECKS.items():
        r = check(validator)
        out[name] = {"passed": bool(r.success),
                     "bad_rows": r.result.get("unexpected_count") or 0,
                     "observed": r.result.get("observed_value")}
    return out


def load(arg: str):
    spark = get_spark("contract")
    if arg == "silver":
        return spark.read.format("delta").load(SILVER)
    return spark.createDataFrame(pd.read_parquet(arg))


if __name__ == "__main__":
    df = load(sys.argv[1])
    print("rows:", df.count())
    for name, r in validate(df).items():
        extra = f"  observed={r['observed']}" if name == "fare_mean_sane" else ""
        print(f"{name:18} passed={r['passed']!s:5}  bad_rows={r['bad_rows']}{extra}")