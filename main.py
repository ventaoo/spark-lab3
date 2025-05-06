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
    # 解析命令行参数
    if len(sys.argv) < 4:
        print("Usage: python main.py <config_path> <data_path> <model_path>")
        sys.exit(1)
    
    config_path, data_path, model_path = sys.argv[1:4]
    
    # 确保输出目录存在
    os.makedirs("./outputs/spark-events", exist_ok=True)
    
    # 初始化日志记录器
    logger = AppLogger(log_file="./outputs/app.log").get_logger()
    logger.info("程序启动，开始加载配置")
    
    try:
        # 加载配置
        config = ConfigLoader.load_yaml_config(config_path)
        logger.info(f"Spark log save to: {config.get('spark', {}).get('config', {}).get('spark.eventLog.dir')}")
        
        # 初始化Spark
        spark_manager = SparkManager(config.get('spark', {}))
        spark = spark_manager.create_session()
        logger.info("Spark session created...")
        
        # 定义常量和参数
        COLUMNS = ["fat_100g", "carbohydrates_100g", "proteins_100g"]
        RANGES = {
            "fat_100g": (0, 100),
            "carbohydrates_100g": (0, 100),
            "proteins_100g": (0, 100),
        }
        K = 3  # 聚类数量
        SAMPLE_FRACTION = 0.1  # 采样比例
        SEED = 42  # 随机种子
        
        # 数据处理
        data_processor = DataProcessor(spark)
        df_clean = data_processor.load_data(data_path)
        df_clean = data_processor.clean_data(df_clean, COLUMNS, RANGES)
        
        # 特征工程
        df_vec = FeatureEngineer.assemble_features(df_clean, COLUMNS)
        df_scaled = FeatureEngineer.scale_features(df_vec)
        
        # 采样数据
        df_sampled = df_scaled.sample(fraction=SAMPLE_FRACTION, seed=SEED)
        
        # 模型训练
        logger.info('Model start training...')
        model = ClusteringModel(k=K, seed=SEED)
        model.train(df_sampled)
        predictions = model.model.transform(df_sampled)
        logger.info(f"Inertia: {model.model.summary.trainingCost:.2f}")
        
        # 模型保存
        try:
            if os.path.exists(model_path):
                # 如果路径存在，使用覆盖模式保存
                model.save(model_path, overwrite=True)
                logger.info(f"模型已覆盖保存: {os.path.abspath(model_path)}")
            else:
                model.save(model_path)
                logger.info(f"模型保存成功: {os.path.abspath(model_path)}")
        except Exception as e:
            logger.error(f"保存模型失败: {e}")
            raise
        
        # 可视化
        visualizer = ClusterVisualizer()
        predictions_pdf = predictions.toPandas()
        
        pca_path = visualizer.plot_pca_clusters(predictions_pdf)
        counts_path = visualizer.plot_cluster_counts(predictions_pdf)
        logger.info(f"可视化结果保存至: {pca_path} 和 {counts_path}")
        
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}", exc_info=True)
        raise
        
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()