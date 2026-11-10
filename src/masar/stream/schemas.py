from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

PING_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("event_ts", TimestampType()),   # event time, from the phone
])
