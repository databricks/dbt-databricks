import os
import re

# In SPOG mode the workspace id can't be parsed from the vanity hostname,
# so the caller provides it via DBT_DATABRICKS_SPOG_WORKSPACE_ID; we also
# append ?o=<wsid> to the cluster paths so the SDK control plane carries
# workspace context. Legacy mode derives the wsid from the hostname.
spog_workspace_id = os.getenv("DBT_DATABRICKS_SPOG_WORKSPACE_ID")

if spog_workspace_id:
    workspace_id = spog_workspace_id
else:
    workspace_re = re.compile(r"^.*-(\d+)\..*$")
    hostname = os.getenv("DBT_DATABRICKS_HOST_NAME", "")
    matches = workspace_re.match(hostname)
    workspace_id = matches.group(1) if matches else ""

cluster_id = os.getenv("TEST_PECO_CLUSTER_ID")
uc_cluster_id = os.getenv("TEST_PECO_UC_CLUSTER_ID")
suffix = f"?o={workspace_id}" if spog_workspace_id else ""
http_path = f"sql/protocolv1/o/{workspace_id}/{cluster_id}{suffix}"
uc_http_path = f"sql/protocolv1/o/{workspace_id}/{uc_cluster_id}{suffix}"

# https://stackoverflow.com/a/72225291/5093960
env_file = os.getenv("GITHUB_ENV", "")
with open(env_file, "a") as myfile:
    myfile.write(f"DBT_DATABRICKS_CLUSTER_HTTP_PATH={http_path}\n")
    myfile.write(f"DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH={uc_http_path}\n")
