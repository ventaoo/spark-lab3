package com.example.datamart

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DataMartApp {

  def main(args: Array[String]): Unit = {
    val cfg = Config.load()

    val spark = SparkSession.builder()
      .appName("DataMartService")
      .master(cfg.sparkMaster)
      .getOrCreate()

    // 1. 读取原始 CSV.GZ 文件
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(cfg.inputPath)

    // 2. 清洗 & 格式转换示例
    val cleanedDF = preprocess(rawDF)

    // 3. 写出为 Parquet 格式，供模型服务消费
    cleanedDF.write
      .mode("overwrite")
      .parquet(cfg.outputPath)

    spark.stop()
  }

  /** 样例预处理：去除空行，填补空值，派生新列 **/
  def preprocess(df: DataFrame): DataFrame = {
    df.na.drop("all") // 整行全空则删除
  }
}
