from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when
from typing import Dict, List

class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def load_data(self, path: str, sep: str = "\t", header: bool = True) -> DataFrame:
        """Load data from file."""
        return self.spark.read.option("sep", sep).option("header", header).option("inferSchema", True).csv(path)
        
    def clean_data(self, df: DataFrame, selected_columns: List[str], valid_ranges: Dict[str, tuple]) -> DataFrame:
        """Clean and filter data."""
        df = df.select([col(c).cast("float") for c in selected_columns]).na.drop()
        
        for column, (lower, upper) in valid_ranges.items():
            df = df.withColumn(
                column, 
                when((col(column) >= lower) & (col(column) <= upper), col(column)).otherwise(None)
            )
            
        return df.na.drop()