
from pyspark.sql.functions import col, when

def clean_maintenance_events(df):
    return (
        df
        .withColumn("downtime_min", when(col("downtime_min").isNull(), 0).otherwise(col("downtime_min")))
        .withColumn("cost_eur", when(col("cost_eur") < 0, 0).otherwise(col("cost_eur")))
    )
