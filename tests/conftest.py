import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="databricks_cluster", type=str)


# Using @pytest.mark.skip_profile('databricks_cluster') uses the 'skip_by_adapter_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "databricks_cluster":
        target = databricks_cluster_target()
    elif profile_type == "databricks_sql_endpoint":
        target = databricks_sql_endpoint_target()
    elif profile_type == "databricks_uc_cluster":
        target = databricks_uc_cluster_target()
    elif profile_type == "databricks_uc_sql_endpoint":
        target = databricks_uc_sql_endpoint_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def databricks_cluster_target():
    return {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": os.getenv(
            "DBT_DATABRICKS_CLUSTER_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        ),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
    }


def databricks_sql_endpoint_target():
    return {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": os.getenv(
            "DBT_DATABRICKS_ENDPOINT_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        ),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
    }


def databricks_uc_cluster_target():
    return {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": os.getenv(
            "DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        ),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
    }


def databricks_uc_sql_endpoint_target():
    return {
        "type": "databricks",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "http_path": os.getenv(
            "DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH", os.getenv("DBT_DATABRICKS_HTTP_PATH")
        ),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        if request.node.get_closest_marker("skip_profile").args[0] == profile_type:
            pytest.skip("skipped on '{profile_type}' profile")
