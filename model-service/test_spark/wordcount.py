from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# 读取文本文件
text = spark.sparkContext.textFile("test.txt")

# 执行 WordCount
word_counts = text.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

# 收集结果
results = word_counts.collect()

# 写入文件（本地文件系统）
with open("wordcount_result.txt", "w", encoding="utf-8") as f:
    for word, count in results:
        f.write(f"{word}\t{count}\n")

# 停止 SparkSession
spark.stop()