import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA

DEFAULT_OUTPUT_DIR = "./outputs"

os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

def plot_pca_clusters(predictions_pdf, output_path=f"{DEFAULT_OUTPUT_DIR}/pca_clusters.png"):
    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(predictions_pdf["scaled_features"].tolist())
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=pca_result[:,0], y=pca_result[:,1],
                    hue=predictions_pdf["prediction"], palette="viridis", alpha=0.6)
    plt.title("聚类结果 (PCA降维)")
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    return output_path
    

def plot_cluster_counts(predictions_pdf, output_path=f"{DEFAULT_OUTPUT_DIR}/cluster_counts.png"):
    plt.figure(figsize=(8, 4))
    sns.countplot(x="prediction", data=predictions_pdf)
    plt.title("各类样本数量分布")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    return output_path
