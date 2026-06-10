"""Parse the SPOG workspace id out of a Databricks http_path."""

from typing import Optional
from urllib.parse import parse_qs


def extract_workspace_id(http_path: Optional[str]) -> Optional[str]:
    """Return the ``?o=<id>`` value from http_path, or None."""
    if not http_path or "?" not in http_path:
        return None
    query = http_path.split("?", 1)[1]
    return parse_qs(query).get("o", [None])[0]
