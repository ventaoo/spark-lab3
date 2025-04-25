# spark-lab

```
    docker-compose up -d
    docker exec -it namenode bash
    hdfs namenode -format

    docker exec namenode hdfs dfs -mkdir -p /lab6/input
    docker exec namenode hdfs dfs -put /app/data/en.openfoodfacts.org.products.csv.gz /lab6/input/
    docker exec namenode hdfs dfs -ls /lab6/input

    docker exec namenode hdfs dfs -chmod -R 777 /lab6/output

    docker exec -it spark bash
    spark-submit /app/main.py \
        /app/config/spark_config.yaml \
        hdfs://namenode:9000/lab6/input/en.openfoodfacts.org.products.csv.gz \
        hdfs://namenode:9000/lab6/output/model
```