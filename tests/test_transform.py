
def test_null_downtime(spark):
    from src.transform import clean_maintenance_events
    df = spark.createDataFrame([(1, None, 10.0)], ["event_id", "downtime_min", "cost_eur"])
    result = clean_maintenance_events(df).collect()[0]
    assert result.downtime_min == 0
