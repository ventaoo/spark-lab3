from pyspark.ml.clustering import KMeans

def train_kmeans(df, k=3, seed=42):
    kmeans = KMeans(k=k, featuresCol="scaled_features", seed=seed)
    return kmeans.fit(df)

def save_model(model, path: str):
    model.save(path)
