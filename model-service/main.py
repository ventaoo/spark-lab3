import sys
import os
import datetime
from src.config.config_loader import ConfigLoader
from src.utils.spark_manager import SparkManager
from src.data.data_processor import DataProcessor
from src.features.feature_engineer import FeatureEngineer
from src.models.clustering_model import ClusteringModel
from src.visualization.plotter import ClusterVisualizer
from src.utils.logger import AppLogger

def main():
    # Parse command line arguments
    if len(sys.argv) < 4:
        print("Usage: python main.py <config_path> <data_path> <model_path>")
        sys.exit(1)
    
    config_path, data_path, model_path = sys.argv[1:4]
    
    # Ensure output directory exists
    os.makedirs("./outputs/spark-events", exist_ok=True)
    
    # Initialize logger
    logger = AppLogger(log_file="./outputs/app.log").get_logger()
    logger.info("Application started, loading configuration...")
    
    try:
        # Load configuration
        config = ConfigLoader.load_yaml_config(config_path)
        logger.info(f"Spark log save to: {config.get('spark', {}).get('config', {}).get('spark.eventLog.dir')}")
        
        # Initialize Spark
        spark_manager = SparkManager(config.get('spark', {}))
        spark = spark_manager.create_session()
        logger.info("Spark session created successfully...")
        
        # Read input data
        df = spark.read.parquet(data_path)
        
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