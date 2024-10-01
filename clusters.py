from databricks_api import DatabricksAPI
from typing import Dict, Set
import need


# list_clusters returns a list of clusters associated to the account
# pass the db object and a set of strings called needs, the clusters returned will
# have the passed 'needs' attributes only if all attributes are needed just pass empty set
def list_clusters(db: DatabricksAPI, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        clusters = db.cluster.list_clusters()["clusters"]
        return {"status": True, "data": need.need_only(needs, clusters)}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> list_clusters E : {e}")
        return {"status": False, "data": str(e)}


# Starts a cluster given the cluster cluster_id
def start_cluster(db: DatabricksAPI, cluster_id: str) -> Dict[str, any]:
    try:
        data = db.cluster.start_cluster(cluster_id=cluster_id)
        return {"status": True, "data": "Cluster has been started"}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> start_cluster E : {e}")
        return {"status": False, "data": str(e)}


# Terminates a cluster given the cluster cluster_id
def terminate_cluster(db: DatabricksAPI, cluster_id: str) -> Dict[str, any]:
    try:
        data = db.cluster.delete_cluster(cluster_id=cluster_id)
        return {"status": True, "data": "Cluster has been Terminated"}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> terminate_cluster E : {e}")
        return {"status": False, "data": str(e)}


# Deletes a cluster given the cluster cluster_id
def delete_cluster(db: DatabricksAPI, cluster_id: str) -> Dict[str, any]:
    try:
        data = db.cluster.permanent_delete_cluster(cluster_id=cluster_id)
        return {"status": True, "data": "Cluster has been deleted"}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> delete_cluster E : {e}")
        return {"status": False, "data": str(e)}


# Creates a cluster
# TODO
