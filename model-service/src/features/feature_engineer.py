from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import DataFrame

class FeatureEngineer:
    @staticmethod
    def assemble_features(df: DataFrame, input_cols: list, output_col: str = "features") -> DataFrame:
        """Assemble features into a vector."""
        assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col)
        return assembler.transform(df)
        
    @staticmethod
    def scale_features(df: DataFrame, input_col: str = "features", output_col: str = "scaled_features") -> DataFrame:
        """Scale features using StandardScaler."""
        scaler = StandardScaler(inputCol=input_col, outputCol=output_col, withStd=True, withMean=True)
        scaler_model = scaler.fit(df)
        return scaler_model.transform(df)