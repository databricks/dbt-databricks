import os

workspace_id = os.getenv("DBT_DATABRICKS_HOST_NAME")[4:18]
cluster_id = os.getenv("TEST_PECO_CLUSTER_ID")
uc_cluster_id = os.getenv("TEST_PECO_UC_CLUSTER_ID")
http_path = f"sql/protocolv1/o/{workspace_id}/{cluster_id}"
uc_http_path = f"sql/protocolv1/o/{workspace_id}/{cluster_id}"

# https://stackoverflow.com/a/72225291/5093960
env_file = os.getenv("GITHUB_ENV")
with open(env_file, "a") as myfile:
    myfile.write(f"DBT_DATABRICKS_CLUSTER_HTTP_PATH={http_path}\n")
    myfile.write(f"DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH={uc_http_path}\n")
