import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.decomposition import PCA
import pandas as pd
from typing import Optional

class ClusterVisualizer:
    def __init__(self, output_dir: str = "./outputs/visualizations"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
    def plot_pca_clusters(self, predictions_pdf: pd.DataFrame, output_filename: str = "pca_clusters.png") -> str:
        """Plot clusters after PCA dimensionality reduction."""
        pca = PCA(n_components=2)
        pca_result = pca.fit_transform(predictions_pdf["scaled_features"].tolist())
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(
            x=pca_result[:,0], y=pca_result[:,1],
            hue=predictions_pdf["prediction"], palette="viridis", alpha=0.6
        )
        plt.title("Cluster Results (PCA Reduced Dimensions)")
        plt.xlabel("Principal Component 1")
        plt.ylabel("Principal Component 2")
        
        output_path = os.path.join(self.output_dir, output_filename)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        
        return output_path
        
    def plot_cluster_counts(self, predictions_pdf: pd.DataFrame, output_filename: str = "cluster_counts.png") -> str:
        """Plot distribution of samples across clusters."""
        plt.figure(figsize=(8, 4))
        sns.countplot(x="prediction", data=predictions_pdf)
        plt.title("Sample Distribution Across Clusters")
        
        output_path = os.path.join(self.output_dir, output_filename)
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        
        return output_path