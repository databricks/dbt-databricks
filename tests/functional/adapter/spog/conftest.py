import os
from copy import deepcopy
from urllib.parse import parse_qsl, urlencode

import pytest

from dbt.adapters.databricks.spog.capabilities import (
    connector_supports_spog,
    sdk_supports_workspace_id,
)
from tests.profiles import get_databricks_cluster_target


@pytest.fixture(scope="session", autouse=True)
def _require_spog_deps():
    if not (connector_supports_spog() and sdk_supports_workspace_id()):
        pytest.skip(
            "SPOG functional tests require databricks-sdk and databricks-sql-connector "
            "with SPOG support installed."
        )


def _workspace_id() -> str:
    workspace_id = os.getenv("TEST_PECO_SPOG_WORKSPACE_ID")
    if not workspace_id:
        raise RuntimeError("SPOG functional tests require TEST_PECO_SPOG_WORKSPACE_ID to be set.")
    return workspace_id


def _spog_host() -> str:
    """Return the SPOG host to point tests at.

    Prefers TEST_PECO_SPOG_HOST so the default (non-SPOG) integration workflow
    can still exercise SPOG routing. Falls back to DBT_DATABRICKS_HOST_NAME,
    which the SPOG-specific workflow already sets to a SPOG vanity URL.
    """
    host = os.getenv("TEST_PECO_SPOG_HOST") or os.getenv("DBT_DATABRICKS_HOST_NAME")
    if not host:
        raise RuntimeError(
            "SPOG functional tests require TEST_PECO_SPOG_HOST or DBT_DATABRICKS_HOST_NAME."
        )
    return host


def _with_workspace_id(http_path: str | None, workspace_id: str) -> str:
    if not http_path:
        raise RuntimeError("SPOG functional tests require an http_path.")

    path, _, query = http_path.partition("?")
    params = dict(parse_qsl(query, keep_blank_values=True))
    params["o"] = workspace_id
    return f"{path}?{urlencode(params)}"


def _spog_target(profile_type: str, workspace_id: str) -> dict:
    target = get_databricks_cluster_target(profile_type)
    target["host"] = _spog_host()
    target["http_path"] = _with_workspace_id(target.get("http_path"), workspace_id)
    return target


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    workspace_id = _workspace_id()
    return _spog_target(profile_type, workspace_id)


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    workspace_id = _workspace_id()
    target = deepcopy(dbt_profile_target)
    target["schema"] = unique_schema

    target["compute"] = {
        "alternate_uc_cluster": {
            "http_path": _spog_target("databricks_uc_cluster", workspace_id)["http_path"]
        }
    }

    profile = {
        "test": {
            "outputs": {
                "default": target,
            },
            "target": "default",
        },
    }

    alternate_warehouse = deepcopy(target)
    alternate_warehouse["compute"] = {
        "alternate_warehouse": {"http_path": target["http_path"]},
        "alternate_warehouse2": {"http_path": target["http_path"]},
        "alternate_warehouse3": {"http_path": target["http_path"]},
    }
    profile["test"]["outputs"]["alternate_warehouse"] = alternate_warehouse

    idle_sessions = deepcopy(alternate_warehouse)
    idle_sessions["connect_max_idle"] = 1
    profile["test"]["outputs"]["idle_sessions"] = idle_sessions

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile
