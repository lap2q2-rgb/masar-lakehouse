import sys
from masar.config import LAKEHOUSE, SILVER
from masar.spark import get_spark

spark = get_spark("small-files")
DEMO = str(LAKEHOUSE / "scratch" / "small_files")


def detail(label: str) -> None:
    row = spark.sql(f"DESCRIBE DETAIL delta.`{DEMO}`").select("numFiles", "sizeInBytes").first()
    print(f"{label}: numFiles={row['numFiles']}  sizeInBytes={row['sizeInBytes']}")


def make_small_files(appends: int = 100, rows_per_append: int = 10) -> None:
    schema = spark.read.format("delta").load(SILVER).schema
    rows = spark.read.format("delta").load(SILVER).limit(appends * rows_per_append).collect()
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(DEMO)
    for i in range(appends):
        batch = rows[i * rows_per_append:(i + 1) * rows_per_append]
        spark.createDataFrame(batch, schema).coalesce(1).write.format("delta").mode("append").save(DEMO)
    detail("after appends")


def maintain() -> None:
    detail("before OPTIMIZE")
    spark.sql(f"OPTIMIZE delta.`{DEMO}`").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
    detail("after OPTIMIZE ")
    print("VACUUM dry run (files it would delete at 168h retention):")
    spark.sql(f"VACUUM delta.`{DEMO}` RETAIN 168 HOURS DRY RUN").show(truncate=False)


if __name__ == "__main__":
    {"make": make_small_files, "maintain": maintain}[sys.argv[1]]()
