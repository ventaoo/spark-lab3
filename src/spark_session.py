from pyspark.sql import SparkSession

def create_spark_session(config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(config.get("app_name", "MySparkApp"))
    for k, v in config.get("config", {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()