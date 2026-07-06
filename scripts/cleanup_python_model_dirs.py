#!/usr/bin/env python3
"""Delete the test service principal's accumulated dbt Python-model notebooks.

dbt writes a notebook per Python model under `/Users/{user}/dbt_python_models/`
and never deletes it, so per-run test schemas accrete until the folder hits
Databricks' 10,000-child cap and `mkdirs` fails. Deletes the whole tree for the
run's principal (dbt recreates what it needs); auth reuses the adapter's
`DatabricksCredentialManager` via the `DBT_DATABRICKS_*` env vars.

Best-effort: always exits 0 (real failures print a `::error::` annotation) so it
never blocks CI. `--dry-run` lists what would be deleted.
"""

from __future__ import annotations

import argparse
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist

from dbt.adapters.databricks.credentials import DatabricksCredentialManager

SUBDIR = "dbt_python_models"


def normalize_host(host: str) -> str:
    """Accept either a bare hostname or a full URL; return a full https URL."""
    return host if host.startswith("http") else f"https://{host}"


def python_models_root(user_name: str) -> str:
    return f"/Users/{user_name}/{SUBDIR}"


def build_client() -> WorkspaceClient:
    """WorkspaceClient authenticated exactly as the adapter authenticates."""
    return DatabricksCredentialManager(
        host=normalize_host(os.environ["DBT_DATABRICKS_HOST_NAME"]),
        client_id=os.environ["DBT_DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DBT_DATABRICKS_CLIENT_SECRET"],
    ).api_client


def purge(client: WorkspaceClient, dry_run: bool = False) -> None:
    """Delete the Python-model notebook tree for the authenticated principal."""
    user_name = client.current_user.me().user_name
    folder = python_models_root(user_name)

    if dry_run:
        try:
            children = list(client.workspace.list(folder))
            # Top-level only (the per-catalog dirs), not a recursive notebook count.
            print(f"[dry-run] would delete {folder} ({len(children)} top-level entries)")
        except ResourceDoesNotExist:
            print(f"[dry-run] {folder} does not exist — nothing to delete")
        return

    print(f"Purging Python-model notebook folder: {folder}")
    try:
        client.workspace.delete(folder, recursive=True)
        print(f"Deleted {folder}")
    except ResourceDoesNotExist:
        print(f"{folder} does not exist — nothing to clean")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the folder and report what would be deleted, without deleting.",
    )
    args = parser.parse_args()

    try:
        purge(build_client(), dry_run=args.dry_run)
    except Exception as e:  # noqa: BLE001 — best-effort; never block the suite
        print(f"::error::Python-model folder cleanup failed (non-fatal): {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
