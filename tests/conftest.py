import pytest

from tests.profiles import get_databricks_cluster_target


pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="databricks_uc_cluster", type=str)


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
    return get_databricks_cluster_target(profile_type)


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
