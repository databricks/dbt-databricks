"""Decision matrix for SPOG preconditions, called from connection.open()."""

from collections.abc import Iterable
from typing import Optional

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.spog import probe
from dbt.adapters.databricks.spog.capabilities import (
    connector_supports_spog,
    sdk_supports_workspace_id,
)
from dbt.adapters.databricks.spog.extract import extract_workspace_id


def check_spog_preconditions(
    *,
    host: str,
    http_paths: Iterable[str],
) -> Optional[str]:
    """Validate SPOG-related configuration.

    Returns the extracted workspace_id (the value of ``?o=`` in http_path,
    when present) or None. Raises :class:`DbtConfigError` only on a
    multi-compute conflict (different ``?o=`` values on http_paths that
    share a connection). Other smells are emitted as warnings.
    """
    # SPOG can't activate on pre-SPOG deps; skip the matrix entirely.
    if not (connector_supports_spog() and sdk_supports_workspace_id()):
        return None

    http_paths_list = list(http_paths)
    extracted: set[str] = {
        ws for p in http_paths_list if (ws := extract_workspace_id(p)) is not None
    }

    if len(extracted) > 1:
        raise DbtConfigError(
            f"Found conflicting workspace-id values across http_paths "
            f"for host {host!r}: {sorted(extracted)}. Every compute used by "
            f"this connection must agree on the workspace id."
        )

    workspace_id: Optional[str] = next(iter(extracted), None)
    metadata = probe.probe_host(host)

    if metadata.host_type == "unified" and workspace_id is None:
        logger.warning(
            f"Host {host!r} looks like a SPOG (unified) workspace but `http_path` "
            f"does not include `?o=<workspace-id>`. Requests may fail to route. "
            f"To opt into SPOG, set `http_path: /sql/1.0/warehouses/<id>?o=<workspace-id>` "
            f"(workspace id is visible in the JDBC/ODBC connection-details panel)."
        )
    elif (
        metadata.host_type is not None
        and metadata.host_type != "unified"
        and workspace_id is not None
    ):
        logger.warning(
            f"`http_path` contains `?o={workspace_id}` but host {host!r} is not "
            f"a SPOG workspace (host_type={metadata.host_type!r}). The `?o=` is "
            f"likely ignored by the routing layer; remove it or use the SPOG "
            f"hostname for this workspace if SPOG routing is intended."
        )

    return workspace_id
