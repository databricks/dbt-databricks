import pytest
from dbt.tests import util
from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)

from tests.functional.adapter.fixtures import (
    RerunSafeMixin,
)
from tests.functional.adapter.relations import fixtures

MATERIALIZATION_VERSIONS = [pytest.param(False, id="v1"), pytest.param(True, id="v2")]


class TestChangeRelationTypesDatabricks(BaseChangeRelationTypeValidator):
    pass


class _TableToViewBase(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"flip_relation.sql": fixtures.flip_relation_as_table_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("flip_relation",)

    def _relation_type(self, project):
        with project.adapter.connection_named("__test_check"):
            relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="flip_relation",
            )
        return relation.type if relation is not None else None

    def _materialize_table_then_view(self, project):
        util.run_dbt(["run"])
        assert self._relation_type(project) == "table"
        util.write_file(fixtures.flip_relation_as_view_sql, "models", "flip_relation.sql")
        util.run_dbt(["run"])

    def _configure_materialization_version(self, project, use_materialization_v2):
        util.update_config_file(
            {"flags": {"use_materialization_v2": use_materialization_v2}},
            project.project_root,
            "dbt_project.yml",
        )


class TestTableConvertsToView(_TableToViewBase):
    @pytest.mark.parametrize(
        "use_materialization_v2",
        MATERIALIZATION_VERSIONS,
    )
    def test_table_converts_to_view(self, project, use_materialization_v2):
        self._configure_materialization_version(project, use_materialization_v2)
        self._materialize_table_then_view(project)
        assert self._relation_type(project) == "view"

    @pytest.mark.parametrize(
        "use_materialization_v2",
        MATERIALIZATION_VERSIONS,
    )
    def test_invalid_view_preserves_table(self, project, use_materialization_v2):
        self._configure_materialization_version(project, use_materialization_v2)
        util.run_dbt(["run"])
        assert self._relation_type(project) == "table"

        util.write_file(fixtures.flip_relation_as_invalid_view_sql, "models", "flip_relation.sql")
        util.run_dbt(["run"], expect_pass=False)

        assert self._relation_type(project) == "table"
        assert (
            project.run_sql("select id from {database}.{schema}.flip_relation", fetch="one")[0] == 1
        )


REPLACEMENTS = [
    pytest.param("table", "view", False, id="table-to-view"),
    pytest.param("table", "view", True, id="table-to-view-v2"),
    pytest.param("materialized_view", "view", False, id="materialized-view-to-view"),
    pytest.param("view", "materialized_view", False, id="view-to-materialized-view"),
    pytest.param(
        "streaming_table", "materialized_view", False, id="streaming-table-to-materialized-view"
    ),
]


def replaced_relation_name(from_type, to_type, use_materialization_v2):
    suffix = "_v2" if use_materialization_v2 else ""
    return f"replaced_{from_type}_to_{to_type}{suffix}"


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestReplacedRelationVisibleToPostHook(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"replaced_relation_source.sql": fixtures.replaced_relation_source_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"assert_cached_as_materialized.sql": fixtures.assert_cached_as_materialized_macro}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return tuple(replaced_relation_name(*p.values) for p in REPLACEMENTS)

    @pytest.mark.parametrize("from_type, to_type, use_materialization_v2", REPLACEMENTS)
    def test_post_hook_sees_replaced_relation(
        self, project, from_type, to_type, use_materialization_v2
    ):
        name = replaced_relation_name(from_type, to_type, use_materialization_v2)
        util.update_config_file(
            {"flags": {"use_materialization_v2": use_materialization_v2}},
            project.project_root,
            "dbt_project.yml",
        )
        util.run_dbt(["run", "--select", "replaced_relation_source"])
        util.write_file(fixtures.replaced_relation_sql(from_type), "models", f"{name}.sql")
        util.run_dbt(["run", "--select", name])

        util.write_file(
            fixtures.replaced_relation_sql(to_type, post_hook=True), "models", f"{name}.sql"
        )
        util.run_dbt(["run", "--select", name])

        count = project.run_sql(f"select count(*) from {{database}}.{{schema}}.{name}", fetch="one")
        assert count[0] == 3


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestChangeRelationTypesParquetDatabricks(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            }
        }
