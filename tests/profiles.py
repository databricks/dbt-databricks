import os
from typing import Any, Dict, Optional


def get_databricks_cluster_target(profile_type: str):
    if profile_type == "databricks_cluster":
        return databricks_cluster_target()
    elif profile_type == "databricks_sql_endpoint":
        return databricks_sql_endpoint_target()
    elif profile_type == "databricks_uc_cluster":
        return databricks_uc_cluster_target()
    elif profile_type == "databricks_uc_sql_endpoint":
        return databricks_uc_sql_endpoint_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")


def _build_databricks_cluster_target(
    http_path: str,
    catalog: Optional[str] = None,
    session_properties: Optional[Dict[str, str]] = None,
):
    profile: Dict[str, Any] = {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": http_path,
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "client_id": os.getenv("DBT_DATABRICKS_CLIENT_ID"),
        "client_secret": os.getenv("DBT_DATABRICKS_CLIENT_SECRET"),
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
        "auth_type": "oauth",
    }
    if catalog is not None:
        profile["catalog"] = catalog
    if session_properties is not None:
        profile["session_properties"] = session_properties
    return profile


def databricks_cluster_target():
    return _build_databricks_cluster_target(
        http_path=os.getenv(
            "DBT_DATABRICKS_CLUSTER_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        )
    )


def databricks_sql_endpoint_target():
    return _build_databricks_cluster_target(
        http_path=os.getenv(
            "DBT_DATABRICKS_ENDPOINT_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        )
    )


def databricks_uc_cluster_target():
    return _build_databricks_cluster_target(
        http_path=os.getenv(
            "DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        ),
        catalog=os.getenv("DBT_DATABRICKS_UC_INITIAL_CATALOG", "main"),
    )


def databricks_uc_sql_endpoint_target():
    return _build_databricks_cluster_target(
        http_path=os.getenv(
            "DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH",
            os.getenv("DBT_DATABRICKS_HTTP_PATH"),
        ),
        catalog=os.getenv("DBT_DATABRICKS_UC_INITIAL_CATALOG", "main"),
    )
