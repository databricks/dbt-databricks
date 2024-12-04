from typing import Any, Optional

from dbt_common.exceptions import DbtRuntimeError

import databricks.sql as dbsql
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.contracts.connection import DEFAULT_QUERY_COMMENT
from dbt.adapters.databricks.__version__ import version as __version__
from dbt.adapters.databricks.credentials import DatabricksCredentials
from dbt.adapters.databricks.logging import logger

DATABRICKS_QUERY_COMMENT = f"""
{{%- set comment_dict = {{}} -%}}
{{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    dbt_databricks_version='{__version__}',
    databricks_sql_connector_version='{dbsql.__version__}',
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}}
{{%- if node is not none -%}}
  {{%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}}
{{% else %}}
  {{# in the node context, the connection name is the node_id #}}
  {{%- do comment_dict.update(connection_name=connection_name) -%}}
{{%- endif -%}}
{{{{ return(tojson(comment_dict)) }}}}
"""


class DatabricksMacroQueryStringSetter(MacroQueryStringSetter):
    def _get_comment_macro(self) -> Optional[str]:
        if self.config.query_comment.comment == DEFAULT_QUERY_COMMENT:
            return DATABRICKS_QUERY_COMMENT
        else:
            return self.config.query_comment.comment


# Number of idle seconds before a connection is automatically closed.
# Updated when idle times of 180s were causing errors
DEFAULT_MAX_IDLE_TIME = 60


def get_max_idle_time(creds: DatabricksCredentials, context: Any) -> int:
    """Get the max idle time for the compute specified for the node.
    If none is specified default will be used."""

    max_idle_time = creds.connect_max_idle or DEFAULT_MAX_IDLE_TIME

    compute_name = get_compute_name(context)
    if compute_name and creds.compute:
        max_idle_time = creds.compute.get(compute_name, {}).get("connect_max_idle", max_idle_time)

    try:
        return int(max_idle_time)
    except ValueError:
        raise DbtRuntimeError(
            f"{max_idle_time} is not a valid value for connect_max_idle. "
            "Must be a number of seconds."
        )


def get_compute_name(context: Any) -> Optional[str]:
    """Get the name of the specified compute resource from the node's config."""
    try:
        return context.config.get("databricks_compute")
    except Exception:
        return None


def get_http_path(creds: DatabricksCredentials, context: Any) -> Optional[str]:
    """Get the http_path for the compute specified for the node.
    If none is specified default will be used.
    """

    # ResultNode *should* have relation_name attr, but we work around a core
    # issue by checking.
    relation_name = getattr(context, "relation_name", "[unknown]")

    # Get the name of the compute resource specified in the node's config.
    # If none is specified return the http_path for the default compute.
    compute_name = get_compute_name(context)
    if not compute_name:
        logger.debug(f"{relation_name} using default compute resource.")
        return creds.http_path

    # Get the http_path for the named compute.
    http_path = None
    if creds.compute:
        http_path = creds.compute.get(compute_name, {}).get("http_path")

    # no http_path for the named compute resource is an error condition
    if not http_path:
        raise DbtRuntimeError(
            f"Compute resource {compute_name} does not exist or "
            f"does not specify http_path, relation: {relation_name}"
        )

    logger.debug(f"{relation_name} using compute resource '{compute_name}'.")

    return http_path
