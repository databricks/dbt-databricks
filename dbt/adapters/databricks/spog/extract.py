"""Parse the workspace id out of a Databricks http_path.

Two URL forms encode the workspace:
- SQL warehouse with explicit ``?o=<id>`` query param (introduced for SPOG so
  warehouse URLs can carry workspace context — see
  databricks/databricks-sql-python#767).
- Cluster path ``/sql/protocolv1/o/<id>/<cluster>`` (workspace already in the path).

Both forms self-identify the workspace, so a cluster path works on SPOG hosts
even without a ``?o=`` query string. ``?o=`` takes precedence when both are
present (highly unusual; only matters if the two disagree).
"""

import re
from typing import Optional
from urllib.parse import parse_qs

_CLUSTER_PATH_WS_ID_RE = re.compile(r"^/?sql/protocolv1/o/(\d+)/")


def extract_workspace_id(http_path: Optional[str]) -> Optional[str]:
    """Return the workspace id from a Databricks http_path, or None.

    Args:
        http_path: The http_path string from a profile or compute config.
            May be None or empty.

    Returns:
        The workspace id as a string, or None when http_path encodes no
        workspace id (e.g. a SQL-warehouse path without ``?o=`` and without
        the cluster ``/o/<id>/`` segment).
    """
    if not http_path:
        return None
    # Prefer the explicit ?o= query param (the SPOG-introduced form).
    ws_id = extract_o_query_param(http_path)
    if ws_id:
        return ws_id
    # Fall back to the workspace id embedded in cluster paths.
    m = _CLUSTER_PATH_WS_ID_RE.match(http_path)
    if m:
        return m.group(1)
    return None


def extract_o_query_param(http_path: Optional[str]) -> Optional[str]:
    """Return the ``?o=<id>`` value from the http_path's query string, or None.

    This is the strict-explicit form. Unlike :func:`extract_workspace_id`, it
    does NOT fall back to the cluster path embedding. Use this when you need
    to detect user-written ``?o=`` configuration specifically — e.g. to
    flag the "``?o=`` on a non-SPOG host" misconfig, which is meaningful only
    for the explicit query-string form (cluster paths always carry the
    workspace id implicitly and that's benign on any host).
    """
    if not http_path or "?" not in http_path:
        return None
    query = http_path.split("?", 1)[1]
    return parse_qs(query).get("o", [None])[0]
