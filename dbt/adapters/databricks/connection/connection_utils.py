from numbers import Number
from typing import Any
from typing import Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt_common.exceptions import DbtRuntimeError


DEFAULT_MAX_IDLE_TIME = 600


def get_max_idle_time(creds: DatabricksCredentials, compute_name: Optional[str] = None) -> int:
    """Get the http_path for the compute specified for the node.
    If none is specified default will be used."""

    max_idle_time = creds.connect_max_idle or DEFAULT_MAX_IDLE_TIME

    if compute_name and creds.compute:
        max_idle_time = creds.compute.get(compute_name, {}).get("connect_max_idle", max_idle_time)

    if isinstance(max_idle_time, Number) or (
        isinstance(max_idle_time, str) and max_idle_time.strip().isnumeric()
    ):
        return int(max_idle_time)
    else:
        raise DbtRuntimeError(
            f"{max_idle_time} is not a valid value for connect_max_idle. "
            "Must be a number of seconds."
        )


def get_compute_name(query_header_context: Any) -> Optional[str]:
    # Get the name of the specified compute resource from the node's
    # config.
    compute_name = None
    if (
        query_header_context
        and hasattr(query_header_context, "config")
        and query_header_context.config
    ):
        compute_name = query_header_context.config.get("databricks_compute", None)
    return compute_name


def get_http_path(
    creds: DatabricksCredentials, compute_name: str, query_header_context: Any
) -> Optional[str]:
    """Get the http_path for the compute specified for the node.
    If none is specified default will be used."""

    # ResultNode *should* have relation_name attr, but we work around a core
    # issue by checking.
    relation_name = getattr(query_header_context, "relation_name", "[unknown]")

    # If there is no node or specified compute name, we return http_path for the default compute.
    if not (query_header_context and compute_name):
        return creds.http_path

    # Get the http_path for the named compute.
    http_path = None
    if creds.compute:
        http_path = creds.compute.get(compute_name, {}).get("http_path", None)

    # no http_path for the named compute resource is an error condition
    if not http_path:
        raise DbtRuntimeError(
            f"Compute resource {compute_name} does not exist or "
            f"does not specify http_path, relation: {relation_name}"
        )

    return http_path
