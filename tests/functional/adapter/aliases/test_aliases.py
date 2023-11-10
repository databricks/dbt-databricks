from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)
from dbt.tests.adapter.aliases import fixtures as dbt_fixtures
import pytest

from tests.functional.adapter.aliases import fixtures as databricks_fixtures


macro_override = {
    "cast.sql": databricks_fixtures.MACROS__CAST_SQL,
    "expect_value.sql": dbt_fixtures.MACROS__EXPECT_VALUE_SQL,
}


class TestDatabricksAliases(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override


class TestDatabricksAliasErrors(BaseAliasErrors):
    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override


class TestDatabricksSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override


class TestDatabricksSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override
