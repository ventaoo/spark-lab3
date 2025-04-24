import sys
import os
from src.config_loader import load_config
from src.spark_session import create_spark_session
from src.data_preprocessing import load_and_clean_data
from src.feature_engineering import assemble_and_scale
from src.clustering_model import train_kmeans, save_model
from src.visualization import plot_pca_clusters, plot_cluster_counts
from src.logger import setup_logger

logger = setup_logger()

if __name__ == "__main__":
    config_path, data_path, model_path = sys.argv[1:4]
    os.makedirs("./outputs/spark-events", exist_ok=True)
    """
    export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///Users/zwt/Desktop/github_project/spark-lab/outputs/spark-events"
    $SPARK_HOME/sbin/start-history-server.sh
    """

    logger.info("程序启动，开始加载配置")
    config = load_config(config_path)
    spark = create_spark_session(config)
    logger.info("Spark session created...")

    COLUMNS = ["fat_100g", "carbohydrates_100g", "proteins_100g"]
    RANGES = {
        "fat_100g": (0, 100),
        "carbohydrates_100g": (0, 100),
        "proteins_100g": (0, 100),
    }

    df_clean = load_and_clean_data(spark, data_path, COLUMNS, RANGES)
    df_scaled = assemble_and_scale(df_clean, COLUMNS)
    df_sampled = df_scaled.sample(fraction=0.1, seed=42)

    logger.info('Model start training...')
    model = train_kmeans(df_sampled, k=3)
    predictions = model.transform(df_sampled)

    logger.info(f"Inertia: {model.summary.trainingCost:.2f}")

    try:
        if os.path.exists(model_path):
            import datetime
            model_path = f"{model_path}_{datetime.datetime.now().strftime('%H:%M:%S')}"
        save_model(model, model_path)
        logger.info(f"模型保存成功: {os.path.abspath(model_path)}")
    except Exception as e:
        logger.error(f"保存模型失败: {e}")

    result_pdf = predictions.sample(fraction=0.1, seed=42).toPandas()
    output_path = plot_pca_clusters(result_pdf)
    logger.info(f"[INFO] 聚类图已保存至：{os.path.abspath(output_path)}")
    output_path = plot_cluster_counts(result_pdf)
    logger.info(f"[INFO] 聚类数量条形图已保存至：{os.path.abspath(output_path)}")
