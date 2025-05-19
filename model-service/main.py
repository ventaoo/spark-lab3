import sys
import os

import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, udf
from pyspark.ml.linalg import Vectors, VectorUDT
from src.config.config_loader import ConfigLoader
from src.models.clustering_model import ClusteringModel
from src.visualization.plotter import ClusterVisualizer
from src.utils.logger import AppLogger


def read_from_postgres(db_config):
    """
    从 PostgreSQL 读取数据，返回 Pandas DataFrame
    """
    engine = create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    
    query = "SELECT * FROM processed_data"
    df_pd = pd.read_sql(query, engine)
    return df_pd


def main():
    # Parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: python main.py <config_path> <model_path>")
        sys.exit(1)

    config_path, model_path = sys.argv[1:3]

    # Ensure output directory exists
    os.makedirs("./outputs/spark-events", exist_ok=True)

    # Initialize logger
    logger = AppLogger(log_file="./outputs/app.log").get_logger()
    logger.info("Application started, loading configuration...")

    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)
        logger.info(f"Spark log save to: {config.get('spark', {}).get('config', {}).get('spark.eventLog.dir')}")

        # 初始化 Spark Session
        spark = SparkSession.builder \
            .appName("ModelTraining") \
            .config("spark.driver.memory", config["spark"]["config"]["spark.driver.memory"]) \
            .config("spark.executor.memory", config["spark"]["config"]["spark.executor.memory"]) \
            .config("spark.executor.cores", str(config["spark"]["config"]["spark.executor.cores"])) \
            .config("spark.sql.shuffle.partitions", str(config["spark"]["config"]["spark.sql.shuffle.partitions"])) \
            .config("spark.sql.execution.arrow.pyspark.enabled",
                    config["spark"]["config"]["spark.sql.execution.arrow.pyspark.enabled"]) \
            .config("spark.dynamicAllocation.enabled",
                    config["spark"]["config"]["spark.dynamicAllocation.enabled"]) \
            .config("spark.ui.showConsoleProgress",
                    config["spark"]["config"]["spark.ui.showConsoleProgress"]) \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "./outputs/spark-events") \
            .getOrCreate()

        logger.info("Spark session created successfully...")

        # 🔁 从数据库读取数据代替 Parquet 文件
        logger.info("Reading data from PostgreSQL...")
        db_config = {
            "dbname": config["database"]["dbname"],
            "user": config["database"]["user"],
            "password": config["database"]["password"],
            "host": config["database"]["host"],
            "port": config["database"]["port"]
        }

        df_pd = read_from_postgres(db_config)

        # 转换为 Spark DataFrame
        df = spark.createDataFrame(df_pd)
        # str -> vector
        str_to_vector_udf = udf(
            lambda s: Vectors.dense([float(x) for x in s.split(',')]) if s else None,
            VectorUDT()
        )
        df = df.withColumn("scaled_features", str_to_vector_udf(col("scaled_features_str")))
        df = df.select("scaled_features")

        logger.info("Data loaded from PostgreSQL and converted to Spark DataFrame.")

        # Model training
        logger.info('Model training started...')
        model = ClusteringModel(k=3, seed=42)
        model.train(df)
        predictions = model.model.transform(df)
        logger.info(f"Inertia: {model.model.summary.trainingCost:.2f}")

        # Save model
        try:
            if os.path.exists(model_path):
                # If path exists, save with overwrite mode
                model.save(model_path, overwrite=True)
                logger.info(f"Model overwritten at: {os.path.abspath(model_path)}")
            else:
                model.save(model_path)
                logger.info(f"Model saved successfully at: {os.path.abspath(model_path)}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise

        # Visualization
        visualizer = ClusterVisualizer()
        predictions_pdf = predictions.toPandas()

        pca_path = visualizer.plot_pca_clusters(predictions_pdf)
        counts_path = visualizer.plot_cluster_counts(predictions_pdf)
        logger.info(f"Visualization results saved to: {pca_path} and {counts_path}")

    except Exception as e:
        logger.error(f"Application error: {str(e)}", exc_info=True)
        raise

    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()