import pytest
from dbt.tests.adapter.aliases import fixtures as dbt_fixtures
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentDatabases,
    BaseSameAliasDifferentSchemas,
)

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
    @pytest.fixture(scope="class", autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_schema_a"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_schema_b"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override


class TestDatabricksSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    @pytest.fixture(scope="class", autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=f"{project.test_schema}_{project.test_schema}_alt",
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def macros(self):
        return macro_override
