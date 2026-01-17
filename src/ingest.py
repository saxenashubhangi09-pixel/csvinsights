
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.appName("MaintenancePipeline").getOrCreate()

def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
