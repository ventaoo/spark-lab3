# spark-lab

```
    docker-compose up -d
    docker exec -it namenode bash
    hdfs namenode -format

    docker exec namenode hdfs dfs -mkdir -p /lab7/input
    docker exec namenode hdfs dfs -mkdir -p /lab7/output
    
    docker exec namenode hdfs dfs -put /app/data/en.openfoodfacts.org.products.csv.gz /lab7/input/
    docker exec namenode hdfs dfs -ls /lab7/input
    docker exec namenode hdfs dfs -ls /lab7/output

    docker exec namenode hdfs dfs -chmod -R 777 /lab7/output

    docker exec -it namenode bash

    docker exec -it spark bash
    spark-submit /app/main.py \
        /app/config/spark_config.yaml \
        hdfs://namenode:9000/lab7/input/en.openfoodfacts.org.products.csv.gz \
        hdfs://namenode:9000/lab7/output/model
```