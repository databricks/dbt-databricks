import os

workspace_id = os.getenv("DBT_DATABRICKS_HOST_NAME")[4:20]
cluster_id = os.getenv("TEST_PECO_CLUSTER_ID")
http_path = f"/sql/protocolv1/o/{workspace_id}/{cluster_id}"

# https://stackoverflow.com/a/72225291/5093960
env_file = os.getenv("GITHUB_ENV")
with open(env_file, "a") as myfile:
    myfile.write(f"DBT_DATABRICKS_CLUSTER_HTTP_PATH={http_path}\n")
