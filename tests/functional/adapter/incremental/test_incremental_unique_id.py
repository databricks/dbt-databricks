import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
)

from tests.functional.adapter.fixtures import MaterializationV2Mixin


class CleanupMixin:
    """Override the upstream clean_up fixture to use class scope.

    The upstream fixture uses function scope (default), which causes it to drop
    the schema after each test method. This creates a race condition in parallel
    test execution where the schema is dropped while other test methods are still
    using it. Using class scope ensures cleanup only happens after all test methods
    in the class complete.
    """

    @pytest.fixture(scope="class", autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)


class TestUniqueKeyDatabricks(CleanupMixin, BaseIncrementalUniqueKey):
    pass


class TestUniqueKeyDatabricksV2(CleanupMixin, MaterializationV2Mixin, BaseIncrementalUniqueKey):
    pass
