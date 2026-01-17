from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

def test_null_downtime(spark):
    from src.transform import clean_maintenance_events

    schema = StructType([
        StructField("event_id", IntegerType(), True),
        StructField("downtime_min", DoubleType(), True),
        StructField("cost_eur", DoubleType(), True),
    ])

    df = spark.createDataFrame(
        [(1, None, 10.0)],
        schema=schema
    )

    result = clean_maintenance_events(df)
    assert result.select("downtime_min").collect()[0][0] == 0
