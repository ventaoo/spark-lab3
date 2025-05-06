from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.sql import DataFrame
from typing import Optional

class ClusteringModel:
    def __init__(self, k: int = 3, seed: Optional[int] = None):
        self.k = k
        self.seed = seed
        self.model: Optional[KMeansModel] = None
        
    def train(self, df: DataFrame, features_col: str = "scaled_features") -> None:
        """Train KMeans model."""
        kmeans = KMeans(k=self.k, featuresCol=features_col, seed=self.seed)
        self.model = kmeans.fit(df)
        
    def save(self, path: str, overwrite: bool = False) -> None:
        """Save trained model.
        
        Args:
            path: Path to save the model
            overwrite: Whether to overwrite existing model
        """
        if self.model is None:
            raise ValueError("Model has not been trained yet")
            
        if overwrite:
            self.model.write().overwrite().save(path)
        else:
            self.model.save(path)
        
    @staticmethod
    def load(path: str) -> KMeansModel:
        """Load saved model."""
        return KMeansModel.load(path)