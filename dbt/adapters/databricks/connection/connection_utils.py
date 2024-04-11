from numbers import Number
from typing import Any
from typing import Optional

from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt_common.exceptions import DbtRuntimeError

# Number of idle seconds before a connection is automatically closed. Only applicable if
# USE_LONG_SESSIONS is true.
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
