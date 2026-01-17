
def test_breakdown_flag(spark):
    from src.fact_table import build_fact_table
    df = spark.createDataFrame(
        [(1, "Unplanned Breakdown"), (2, "Planned Maintenance")],
        ["event_id", "reason"]
    )
    result = build_fact_table(df)
    assert result.filter("is_breakdown = true").count() == 1
