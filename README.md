# spark-lab

```
    sbt clean assembly
    
    docker compose up -d --build  
   
    python model-service/main.py model-service/config/spark_config.yaml data-mart/data/output_parquet ./models
```