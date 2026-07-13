import os

import pytest

from tests.profiles import get_databricks_cluster_target

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    # Use DBT_DATABRICKS_PROFILE env var if set, otherwise default to databricks_uc_sql_endpoint
    default_profile = os.environ.get("DBT_DATABRICKS_PROFILE", "databricks_uc_sql_endpoint")
    parser.addoption("--profile", action="store", default=default_profile, type=str)


# @pytest.mark.skip_profile(...) and @pytest.mark.skip_kernel are enforced by the
# autouse fixtures below (skip_by_profile_type, skip_on_kernel_backend).
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
    config.addinivalue_line(
        "markers",
        "skip_kernel: skip test when running against the connector kernel backend",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    return get_databricks_cluster_target(profile_type)


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")


@pytest.fixture(autouse=True)
def skip_on_kernel_backend(request):
    # The connector kernel backend (DBT_DATABRICKS_USE_KERNEL=1) routes over SEA,
    # which is warehouse-only. Tests that pin a model to an all-purpose-cluster
    # http_path or compute cannot connect through it, so they opt out with
    # @pytest.mark.skip_kernel.
    if os.environ.get("DBT_DATABRICKS_USE_KERNEL") == "1" and request.node.get_closest_marker(
        "skip_kernel"
    ):
        pytest.skip("skipped on the connector kernel backend (SEA is warehouse-only)")


# The profile dictionary, used to write out profiles.yml. It will pull in updates
# from two separate sources, the 'profile_target' and 'profiles_config_update'.
# The second one is useful when using alternative targets, etc.
@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    profile = {
        "test": {
            "outputs": {
                "default": {},
            },
            "target": "default",
        },
    }
    target = dbt_profile_target
    target["schema"] = unique_schema

    # For testing model-level compute override
    target["compute"] = {
        "alternate_uc_cluster": {
            "http_path": get_databricks_cluster_target("databricks_uc_cluster")["http_path"]
        }
    }
    profile["test"]["outputs"]["default"] = target

    alternate_warehouse = target.copy()
    alternate_warehouse["compute"] = {
        "alternate_warehouse": {"http_path": dbt_profile_target["http_path"]},
        "alternate_warehouse2": {"http_path": dbt_profile_target["http_path"]},
        "alternate_warehouse3": {"http_path": dbt_profile_target["http_path"]},
    }
    profile["test"]["outputs"]["alternate_warehouse"] = alternate_warehouse

    idle_sessions = alternate_warehouse.copy()
    idle_sessions["connect_max_idle"] = 1
    profile["test"]["outputs"]["idle_sessions"] = idle_sessions

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile
