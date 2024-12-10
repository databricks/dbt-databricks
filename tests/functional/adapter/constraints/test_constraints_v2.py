import pytest

from dbt.tests import util
from dbt.tests.adapter.constraints import fixtures
from dbt.tests.adapter.constraints.test_constraints import (
    _find_and_replace,
    _normalize_whitespace,
)
from tests.functional.adapter.constraints import fixtures as override_fixtures
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class BaseV2ConstraintSetup:
    @pytest.fixture(scope="class")
    def override_config(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self, override_config):
        config = {"flags": {"use_materialization_v2": True}}
        config.update(override_config)
        return config

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return override_fixtures.expected_sql

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": fixtures.foreign_key_model_sql,
            "constraints_schema.yml": fixtures.model_fk_constraint_schema_yml.replace(
                "text", "string"
            ),
        }

    def test__constraints_ddl(self, project, expected_sql):
        results, log = util.run_dbt(["run", "--debug"])
        assert len(results) >= 1

        generated_sql_generic = _find_and_replace(log, "my_model", "<model_identifier>")
        generated_sql_generic = _find_and_replace(
            generated_sql_generic, "foreign_key_model", "<foreign_key_model_identifier>"
        )

        assert _normalize_whitespace(expected_sql) == _normalize_whitespace(generated_sql_generic)


class TestConstraintQuotedColumn(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": fixtures.model_quoted_column_schema_yml.replace(
                "text", "string"
            ).replace('"from"', "`from`"),
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier> (
    `from` string not null,
    id integer not null comment 'hello',
    date_day string
)
    using delta
"""

    def test__constraints_ddl(self, project, expected_sql):
        results, logs = util.run_dbt_and_capture(["run", "--debug", "-s", "+my_model"])
        assert len(results) >= 1
        generated_sql_generic = _find_and_replace(logs, "my_model", "<model_identifier>")
        normalized = _normalize_whitespace(generated_sql_generic)
        assert _normalize_whitespace(expected_sql) in normalized
        assert _normalize_whitespace("ALTER TABLE <model_identifier> ADD CONSTRAINT") in normalized
        assert _normalize_whitespace("CHECK (`from` = 'blue')") in normalized


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsDdlEnforcement(BaseV2ConstraintSetup):
    pass
