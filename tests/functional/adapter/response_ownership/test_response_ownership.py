import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures as incremental_fixtures
from tests.functional.adapter.response_ownership import fixtures
from tests.functional.adapter.row_filters import fixtures as row_filter_fixtures


def _response_value(response, key):
    if isinstance(response, dict):
        return response.get(key)
    return getattr(response, key, None)


def _assert_counted_response(result, expected_rows_affected):
    response = result.adapter_response
    actual = (
        _response_value(response, "rows_affected"),
        _response_value(response, "_message"),
    )
    assert actual == (expected_rows_affected, f"OK {expected_rows_affected}")


@pytest.mark.skip_profile("databricks_cluster")
class TestV2CreateResponseOwnership(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("v2_create_response",)

    @pytest.fixture(scope="class")
    def models(self):
        return {"v2_create_response.sql": fixtures.v2_create_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}

    def test_insert_owns_initial_create_response(self, project):
        result = util.run_dbt(["run", "--select", "v2_create_response"])[0]

        rows = project.run_sql("select id, msg from v2_create_response order by id", fetch="all")
        assert [tuple(row) for row in rows] == [(1, "hello"), (2, "goodbye")]
        _assert_counted_response(result, 2)


@pytest.mark.skip_profile("databricks_cluster")
class TestV2SafeReplacementResponseOwnership(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("v2_safe_replace_response",)

    @pytest.fixture(scope="class")
    def models(self):
        return {"v2_safe_replace_response.sql": fixtures.v2_safe_replace_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True},
        }

    def test_insert_owns_safe_replacement_response(self, project):
        util.run_dbt(["run", "--select", "v2_safe_replace_response"])
        first_id = project.run_sql(
            "describe detail {database}.{schema}.v2_safe_replace_response", fetch="all"
        )[0]["id"]

        result = util.run_dbt(["run", "--select", "v2_safe_replace_response"])[0]

        detail = project.run_sql(
            "describe detail {database}.{schema}.v2_safe_replace_response", fetch="all"
        )[0]
        rows = project.run_sql(
            "select id, msg from v2_safe_replace_response order by id", fetch="all"
        )
        assert detail["id"] != first_id
        assert [tuple(row) for row in rows] == [(1, "hello"), (2, "goodbye")]
        _assert_counted_response(result, 2)


@pytest.mark.skip_profile("databricks_cluster")
class TestV1ConstraintResponseOwnership(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("primary_key_constraint_sql",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "primary_key_constraint_sql.sql": incremental_fixtures.primary_key_constraint_sql,
            "schema.yml": incremental_fixtures.schema_with_single_column_primary_key_constraint,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    def test_constraint_alter_does_not_overwrite_incremental_response(self, project):
        util.run_dbt(["run", "--select", "primary_key_constraint_sql"])
        util.write_file(
            incremental_fixtures.schema_with_composite_primary_key_constraint,
            "models",
            "schema.yml",
        )

        result = util.run_dbt(["run", "--select", "primary_key_constraint_sql"])[0]

        constraints = project.run_sql(
            """
            select constraint_name, column_name
            from {database}.information_schema.key_column_usage
            where constraint_schema = '{schema}'
              and table_name = 'primary_key_constraint_sql'
            order by ordinal_position
            """,
            fetch="all",
        )
        rows = project.run_sql(
            "select id, version, msg from primary_key_constraint_sql", fetch="all"
        )
        assert [tuple(row) for row in constraints] == [
            ("pk_model_updated", "id"),
            ("pk_model_updated", "version"),
        ]
        assert [tuple(row) for row in rows] == [
            (1, 1, "hello"),
            (1, 1, "hello"),
        ]
        _assert_counted_response(result, 1)


@pytest.mark.skip_profile("databricks_cluster")
class TestV1RowFilterResponseOwnership(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("base_model",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_model.sql": row_filter_fixtures.base_model_sql.replace("table", "incremental"),
            "schema.yml": row_filter_fixtures.model_with_row_filter,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @staticmethod
    def _create_filter_udfs(project):
        project.run_sql(
            f"""
            create or replace function {project.database}.{project.test_schema}.region_filter(
                region string
            )
            returns boolean
            return region = 'region_a'
            """
        )
        project.run_sql(
            f"""
            create or replace function {project.database}.{project.test_schema}.user_filter(
                user_id string
            )
            returns boolean
            return user_id = 'user1'
            """
        )

    def test_row_filter_alter_does_not_overwrite_incremental_response(self, project):
        self._create_filter_udfs(project)
        util.run_dbt(["run", "--select", "base_model"])
        util.write_file(row_filter_fixtures.model_updated_filter, "models", "schema.yml")

        result = util.run_dbt(["run", "--select", "base_model"])[0]

        filters = project.run_sql(
            """
            select filter_name, target_columns
            from {database}.information_schema.row_filters
            where table_schema = '{schema}'
              and table_name = 'base_model'
            """,
            fetch="all",
        )
        rows = project.run_sql("select user_id, region, amount from base_model", fetch="all")
        assert len(filters) == 1
        assert filters[0]["filter_name"].lower().endswith(".user_filter")
        assert "user_id" in filters[0]["target_columns"].lower()
        assert [tuple(row) for row in rows] == [
            ("user1", "region_a", 100),
            ("user1", "region_a", 100),
        ]
        _assert_counted_response(result, 1)
