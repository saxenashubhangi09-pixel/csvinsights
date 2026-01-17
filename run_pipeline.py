
from src.ingest import get_spark, read_csv
from src.transform import clean_maintenance_events
from src.fact_table import build_fact_table

spark = get_spark()
df = read_csv(spark, "data/raw/maintenance_events.csv")
df_clean = clean_maintenance_events(df)
fact = build_fact_table(df_clean)

fact.write.mode("overwrite").parquet("data/processed/fact_table.parquet")
fact.write.mode("overwrite").csv("data/processed/fact_table.csv", header=True)
spark.stop()
