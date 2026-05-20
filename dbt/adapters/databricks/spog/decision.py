"""Decision matrix for SPOG preconditions, called from connection.open()."""

from collections.abc import Iterable
from typing import Optional

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.spog.capabilities import (
    connector_supports_spog,
    sdk_supports_workspace_id,
)
from dbt.adapters.databricks.spog.extract import extract_o_query_param, extract_workspace_id
from dbt.adapters.databricks.spog.probe import probe_host


def check_spog_preconditions(
    *,
    host: str,
    http_paths: Iterable[str],
) -> Optional[str]:
    """Apply the spec §8 decision matrix.

    Returns the extracted workspace_id when the SPOG path is active; returns
    None when the legacy path is active. Raises DbtConfigError on
    misconfiguration.

    The caller is `DatabricksConnectionManager.open()`. `http_paths` is the
    iterable of all http_path values in play for this connection (the default
    http_path plus any per-compute paths). All paths must agree on the
    workspace_id (or all lack `?o=`).
    """
    http_paths_list = list(http_paths)
    # `extracted` is the workspace_id derived from any source (?o= or
    # cluster /o/<id>/). Used to decide whether SPOG context is available.
    extracted: set[str] = {
        ws for p in http_paths_list if (ws := extract_workspace_id(p)) is not None
    }
    # `explicit_o` is the strict ?o= query-string form only. Used to detect
    # user-written "?o= on a non-SPOG host" misconfig (cluster paths embed
    # the workspace id implicitly and that's benign on any host).
    explicit_o: set[str] = {
        ws for p in http_paths_list if (ws := extract_o_query_param(p)) is not None
    }

    if len(extracted) > 1:
        raise DbtConfigError(
            f"Found conflicting workspace-id values across http_paths "
            f"for host {host!r}: {sorted(extracted)}. Every compute used by "
            f"this connection must agree on the workspace id."
        )

    workspace_id: Optional[str] = next(iter(extracted), None)
    metadata = probe_host(host)

    if metadata.host_type == "unified":
        if workspace_id is None:
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace, but `http_path` "
                f"does not include `?o=<workspace-id>`. Update your profiles.yml "
                f"to: `http_path: /sql/1.0/warehouses/<id>?o=<workspace-id>`. "
                f"The workspace id is visible in the Databricks UI when copying "
                f"the JDBC/ODBC connection details."
            )
        if not connector_supports_spog():
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace. The installed "
                f"`databricks-sql-connector` does not support SPOG (requires "
                f">= 4.2.6). Upgrade with: "
                f"`pip install 'databricks-sql-connector>=4.2.6'`."
            )
        if not sdk_supports_workspace_id():
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace. The installed "
                f"`databricks-sdk` does not support `workspace_id` on Config "
                f"(requires >= 0.104.0). Upgrade with: "
                f"`pip install 'databricks-sdk>=0.104.0'`."
            )
        return workspace_id

    # Only raise on non-SPOG host if there's an EXPLICIT ?o= the user wrote.
    # A workspace_id derived from a cluster path's /o/<id>/ embed is implicit
    # and benign — cluster paths carry their workspace context naturally and
    # work on any host.
    if metadata.host_type is not None and explicit_o:
        bad = next(iter(explicit_o))
        raise DbtConfigError(
            f"`http_path` contains `?o={bad}` but host {host!r} is not "
            f"a SPOG workspace (host_type={metadata.host_type!r}). Either "
            f"change `host` to the SPOG hostname for this workspace, or remove "
            f"`?o=` from `http_path`."
        )

    # Legacy host with no explicit ?o= OR probe failed: return workspace_id
    # as-is. When the probe failed but ?o= is present, be permissive so the
    # caller still plumbs the id through (a WARN was already logged in
    # probe_host).
    return workspace_id
