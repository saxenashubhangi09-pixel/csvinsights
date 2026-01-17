
from pyspark.sql.functions import when

def build_fact_table(df):
    return (
        df
        .withColumn("is_breakdown",
            when(df.reason == "Unplanned Breakdown", True).otherwise(False)
        )
        .select(
            "event_id",
            "maintenance_type",
            "reason",
            "downtime_min",
            "cost_eur",
            "is_breakdown"
        )
    )
