from masar.spark import get_spark


def add_constraints() -> None:
    spark = get_spark("enforce-silver")
    spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT positive_fare CHECK (fare_sar > 0)")
    spark.sql("ALTER TABLE silver.trips ADD CONSTRAINT valid_duration CHECK (ended_at > started_at)")
    spark.sql("SHOW TBLPROPERTIES silver.trips").show(truncate=False)


if __name__ == "__main__":
    add_constraints()
