from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def load_and_clean_data(spark, path: str, selected_columns: list, valid_ranges: dict) -> DataFrame:
    df = spark.read.option("sep", "\t").option("header", True).option("inferSchema", True).csv(path)
    df = df.select([col(c).cast("float") for c in selected_columns]).na.drop()
    for column, (lower, upper) in valid_ranges.items():
        df = df.withColumn(column, when((col(column) >= lower) & (col(column) <= upper), col(column)).otherwise(None))
    return df.na.drop()