from pyspark.sql import SparkSession
from typing import Dict

class SparkManager:
    def __init__(self, config: Dict):
        self.config = config
        
    def create_session(self) -> SparkSession:
        """Create and return a SparkSession based on the provided configuration."""
        builder = SparkSession.builder.appName(self.config.get("app_name", "MySparkApp"))
        
        for k, v in self.config.get("config", {}).items():
            builder = builder.config(k, v)
            
        return builder.getOrCreate()