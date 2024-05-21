import pytest

from dbt.tests.adapter.simple_copy.test_simple_copy import EmptyModelsArentRunBase
from dbt.tests.adapter.simple_copy.test_simple_copy import SimpleCopyBase


# Tests with materialized_views, which only works for SQL Warehouse
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestSimpleCopyBase(SimpleCopyBase):
    pass


# Test uses information_schema, not present in HMS
@pytest.mark.skip_profile("databricks_cluster")
class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass
