import pandas as pd
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import matplotlib.colors as mcolors


# SETUP AND GET PCA DATA

#UNCOMMENT WHICHEVER ONE YOU WANT TO SEE 

df = pd.read_csv("Base.txt", header=None)
#df = pd.read_csv("Demographic.txt", header=None)
#df = pd.read_csv("Health.txt", header=None)
#df = pd.read_csv("Strongest.txt", header=None)
#df = pd.read_csv("Utilization2.txt", header=None)

#BASE
df.columns = pd.Index([
    "cluster_id",
    "point_id",
    "age",
    "bmi",
    "children",
    "annual_income",
    "chronic_diseases",
    "doctor_visits",
    "hospitalizations",
    "alcohol_consumption",
    "cost"
])

# #DEMOGRAPHIC
# df.columns = pd.Index([
#     "cluster_id",
#     "point_id",
#     "age",
#     "children",
#     "annual_income",
#     "cost"
# ])

# #HEALTH
# df.columns = pd.Index([
#     "cluster_id",
#     "point_id",
#     "age",
#     "bmi",
#     "children",
#     "chronic_diseases",
#     "alcohol_consumption",
#     "cost"
# ])


# #STRONGEST
# df.columns = pd.Index([
#     "cluster_id",
#     "point_id",
#     "age",
#     "bmi",
#     "chronic_diseases",
#     "hospitalizations",
#     "cost"
# ])

# #UTILIZATION
# df.columns = pd.Index([
#     "cluster_id",
#     "point_id",
#     "chronic_diseases",
#     "doctor_visits",
#     "hospitalizations",
#     "cost"
# ])

#PCA INFO
X = df.drop(columns=["cluster_id", "point_id", "cost"])

pca = PCA(n_components=2) #n_components=2
pca_result = pca.fit_transform(X)

explained = pca.explained_variance_ratio_

print(pd.DataFrame({
    'PCA': [f'PCA{i+1}' for i in range(len(explained))],
    'Explained Var': explained.round(4)
}))

loadings = pd.DataFrame(
    pca.components_.T,
    columns=["PCA1", "PCA2"],
    index=X.columns
)

print(loadings.sort_values("PCA1", key=abs, ascending=False))


#CLUSTER + PCA GRAPH
df["PCA1"] = pca_result[:, 0]
df["PCA2"] = pca_result[:, 1]

centroids = df.drop(columns=["point_id", "cost", "PCA1", "PCA2"]).groupby("cluster_id").mean()
centroids_pca = pca.transform(centroids)

centroids_df = pd.DataFrame({
    "PCA1": centroids_pca[:, 0],
    "PCA2": centroids_pca[:, 1],
    "cluster_id": centroids.index
})

cluster_ids = sorted(df["cluster_id"].unique())
n_clusters = len(cluster_ids)

norm = mcolors.Normalize(vmin=df["cluster_id"].min(), vmax=df["cluster_id"].max())
cmap = plt.get_cmap("Set1", n_clusters)

plt.figure(figsize=(10, 7))

plt.scatter(df["PCA1"], df["PCA2"], c=df["cluster_id"], cmap=cmap,
            norm=norm, alpha=0.6, s=18)

plt.scatter(centroids_df["PCA1"], centroids_df["PCA2"],
            c=centroids_df["cluster_id"], cmap=cmap,
            norm=norm, s=200, marker="X", edgecolors="black", linewidths=0.8,
            zorder=5)

legend_handles = []
for i, cid in enumerate(cluster_ids):
    color = cmap(i)
    patch = mpatches.Patch(color=color, label=f"Cluster {cid}")
    legend_handles.append(patch)

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

#Average cost by cluster
cluster_cost = df.groupby("cluster_id")["cost"].mean()

plt.figure(figsize=(8,5))
cluster_cost.plot(kind="bar")
plt.ylabel("Average Cost")
plt.xlabel("Cluster")
plt.title("Average Insurance Cost by Cluster")
plt.show()


#cluster data w/ pca
centroids = df.groupby("cluster_id").mean(numeric_only=True)
print(centroids)