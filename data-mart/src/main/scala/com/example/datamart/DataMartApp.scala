package com.example.datamart

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.linalg.Vectors

object DataMartApp {

  def main(args: Array[String]): Unit = {
    val cfg = Config.load()

    val spark = SparkSession.builder()
      .appName("DataMartService")
      .master(cfg.sparkMaster)
      .getOrCreate()

    import spark.implicits._

    // 1. 读取原始 CSV.GZ 文件
    val rawDF = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(cfg.inputPath)

    // 2. 数据清洗和预处理
    val cleanedDF = preprocess(rawDF)

    // 3. 特征工程
    val processedDF = processFeatures(cleanedDF)

    // 4. 采样并写出为 Parquet 格式
    processedDF.sample(0.1, seed = 42)
      .write
      .mode("overwrite")
      .parquet(cfg.outputPath)

    spark.stop()
  }

  /** 数据预处理 */
  def preprocess(df: DataFrame): DataFrame = {
    // 选择目标列并转换类型
    val selectedColumns = Seq("fat_100g", "carbohydrates_100g", "proteins_100g")
    
    df.select(selectedColumns.map(col): _*)
      .na.drop("all") // 删除全空行
      .withColumn("fat_100g", col("fat_100g").cast(FloatType))
      .withColumn("carbohydrates_100g", col("carbohydrates_100g").cast(FloatType))
      .withColumn("proteins_100g", col("proteins_100g").cast(FloatType))
      .na.drop() // 删除包含空值的行
  }

  /** 特征处理 */
  def processFeatures(df: DataFrame): DataFrame = {
    // 定义有效范围
    val validRanges = Map(
      "fat_100g" -> (0f, 100f),
      "carbohydrates_100g" -> (0f, 100f),
      "proteins_100g" -> (0f, 100f)
    )

    // 过滤异常值
    var filteredDF = df
    for ((colName, (lower, upper)) <- validRanges) {
        filteredDF = filteredDF.withColumn(
            colName,
            when(col(colName).between(lower, upper), col(colName)).otherwise(lit(null))
        )
    }
    filteredDF = filteredDF.na.drop()

    // 向量化
    val assembler = new VectorAssembler()
      .setInputCols(Array("fat_100g", "carbohydrates_100g", "proteins_100g"))
      .setOutputCol("features")

    val assembledDF = assembler.transform(filteredDF)

    // 标准化
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(assembledDF)
    scalerModel.transform(assembledDF)
  }
}