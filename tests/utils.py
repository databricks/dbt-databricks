import os
from typing import Dict, Optional


def build_databricks_cluster_profile(
    http_path: str,
    catalog: Optional[str] = None,
    session_properties: Optional[Dict[str, str]] = None,
):
    profile = {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": http_path,
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
    }
    if catalog is not None:
        # TODO: catalog should be set as 'catalog' or 'database'
        #       instead of using 'session_properties'
        # profile['catalog'] = catalog
        if session_properties is not None:
            session_properties["databricks.catalog"] = catalog
        else:
            session_properties = {"databricks.catalog": catalog}
    if session_properties is not None:
        profile["session_properties"] = session_properties  # type: ignore[assignment]
    return profile
