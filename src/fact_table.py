from pyspark.sql import functions as F

def build_fact_table(df):
    df = df.withColumn(
        "is_breakdown",
        F.when(F.col("reason") == "Unplanned Breakdown", True).otherwise(False)
    )

    required_cols = [
        "event_id",
        "maintenance_type",
        "reason",
        "downtime_min",
        "cost_eur",
        "is_breakdown",
    ]

    existing_cols = [c for c in required_cols if c in df.columns]

    return df.select(*existing_cols)
