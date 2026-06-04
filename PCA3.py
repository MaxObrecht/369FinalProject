import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import seaborn as sns
from scipy import stats
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import matplotlib.colors as mcolors


# SETUP AND GET PCA DATA
df = pd.read_csv("Trial6.txt", header=None)

df.columns = pd.Index([
    "cluster_id",
    "point_id",
    "age",
    "gender",
    "bmi",
    "children",
    "smoker",
    "annual_income",
    "chronic_diseases",
    "doctor_visits",
    "hospitalizations",
    "alcohol_consumption",
    "cost"
])


X = df.drop(columns=["cluster_id", "point_id", "cost"])

pca = PCA() #n_components=2
pca_result = pca.fit_transform(X)

explained = pca.explained_variance_ratio_

print(pd.DataFrame({
    'PC': [f'PC{i+1}' for i in range(len(explained))],
    'Explained Var': explained.round(4)
}))

loadings = pd.DataFrame(
    pca.components_.T,
    columns=["PC1", "PC2", "PC3", "PC4", "PC5", "PC6", "PC7", "PC8", "PC9", "PC10",],
    index=X.columns
)

print(loadings.sort_values("PC1", key=abs, ascending=False))





#K MEANS CLUSTERING DATA
df["pca_1"] = pca_result[:, 0]
df["pca_2"] = pca_result[:, 1]

centroids = df.drop(columns=["point_id", "cost", "pca_1", "pca_2"]).groupby("cluster_id").mean()
centroids_pca = pca.transform(centroids.values)

centroids_df = pd.DataFrame({
    "pca_1": centroids_pca[:, 0],
    "pca_2": centroids_pca[:, 1],
    "cluster_id": centroids.index
})

cluster_ids = sorted(df["cluster_id"].unique())
n_clusters = len(cluster_ids)

# Normalize cluster IDs to [0, 1] for colormap (same mapping as scatter)
norm = mcolors.Normalize(vmin=df["cluster_id"].min(), vmax=df["cluster_id"].max())
cmap = cm.get_cmap("Set1", n_clusters)

plt.figure(figsize=(10, 7))

plt.scatter(df["pca_1"], df["pca_2"], c=df["cluster_id"], cmap=cmap,
            norm=norm, alpha=0.6, s=18)

plt.scatter(centroids_df["pca_1"], centroids_df["pca_2"],
            c=centroids_df["cluster_id"], cmap=cmap,
            norm=norm, s=200, marker="X", edgecolors="black", linewidths=0.8,
            zorder=5)

# Build legend: one entry per cluster (color swatch + centroid marker)
legend_handles = []
for i, cid in enumerate(cluster_ids):
    color = cmap(i)  # sample by index, not through norm
    patch = mpatches.Patch(color=color, label=f"Cluster {cid}")
    legend_handles.append(patch)

# Append a generic centroid marker entry
centroid_handle = plt.scatter([], [], c="gray", s=120, marker="X",
                               edgecolors="black", linewidths=0.8, label="Centroids")
legend_handles.append(centroid_handle)

plt.legend(
    handles=legend_handles,
    title="Clusters",
    title_fontsize=10,
    fontsize=8,
    loc="best",
    framealpha=0.9
)

plt.xlabel("PCA 1")
plt.ylabel("PCA 2")
plt.title("PCA1 vs PCA2 with centroids")
plt.grid(True)
plt.show()

cluster_cost = df.groupby("cluster_id")["cost"].mean()








# PCA1 plot by cost
plt.figure(figsize=(10, 7))

scatter = plt.scatter(
    df["pca_1"],
    df["cost"],
    alpha=0.65
)

m, b = np.polyfit(df["pca_1"], df["cost"], 1)
plt.plot(np.sort(df["pca_1"]), m * np.sort(df["pca_1"]) + b, color="black", linewidth=1.5)

plt.xlabel(f"PCA 1")
plt.ylabel(f"cost")
plt.title("Cost vs PCA1")
plt.grid(True)
plt.show()




#Average cost by cluster
cluster_cost = df.groupby("cluster_id")["cost"].mean()

plt.figure(figsize=(8,5))
cluster_cost.plot(kind="bar")
plt.ylabel("Average Cost")
plt.xlabel("Cluster")
plt.title("Average Insurance Cost by Cluster")
plt.show()