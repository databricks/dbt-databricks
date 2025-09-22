import os

import pytest

from dbt.tests import util
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalColumnTags(MaterializationV2Mixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.column_tags_a,
        }

    def test_changing_column_tags(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.column_tags_b, "models", "schema.yml")
        util.run_dbt(["run"])

        # Check final column tags
        results = project.run_sql(
            f"""
            select column_name, tag_name, tag_value
            from `system`.`information_schema`.`column_tags`
            where schema_name = '{project.test_schema}'
            and table_name = 'merge_update_columns'
            order by column_name, tag_name
            """,
            fetch="all",
        )

        assert len(results) == 4

        # Convert to dict for easier assertions
        tags_dict = {}
        for row in results:
            col = row.column_name
            if col not in tags_dict:
                tags_dict[col] = {}
            tags_dict[col][row.tag_name] = row.tag_value

        # Verify expected final state with "set only" behavior
        expected_tags = {
            "id": {"pii": "false", "source": "application"},  # source updated
            "msg": {"pii": "true"},  # new column tag
            "color": {"pii": "false"},  # persisted from original
        }
        assert tags_dict == expected_tags


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalPythonColumnTags(TestIncrementalColumnTags):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns.py": fixtures.column_tags_python_model,
            "schema.yml": self.column_tags_a_with_sql_warehouse(),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # For SQL Warehouse profiles, Python models need a cluster http_path
        cluster_http_path = os.getenv("DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH") or os.getenv(
            "DBT_DATABRICKS_CLUSTER_HTTP_PATH"
        )
        config = {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+create_notebook": "true",
                "+submission_method": "all_purpose_cluster",
            },
        }
        if cluster_http_path:
            config["models"]["+http_path"] = cluster_http_path
        return config

    def column_tags_a_with_sql_warehouse(self):
        return """
version: 2

models:
  - name: merge_update_columns
    columns:
        - name: id
          databricks_tags:
            pii: "false"
            source: "system"
        - name: msg
        - name: color
          databricks_tags:
            pii: "false"
"""

    def column_tags_b_with_sql_warehouse(self):
        return """
version: 2

models:
  - name: merge_update_columns
    columns:
        - name: id
          databricks_tags:
            pii: "false"
            source: "application"
        - name: msg
          databricks_tags:
            pii: "true"
        - name: color
"""

    def test_changing_column_tags(self, project):
        util.run_dbt(["run"])
        util.write_file(self.column_tags_b_with_sql_warehouse(), "models", "schema.yml")
        util.run_dbt(["run"])

        # Check final column tags
        results = project.run_sql(
            f"""
            select column_name, tag_name, tag_value
            from `system`.`information_schema`.`column_tags`
            where schema_name = '{project.test_schema}'
            and table_name = 'merge_update_columns'
            order by column_name, tag_name
            """,
            fetch="all",
        )

        assert len(results) == 4

        # Convert to dict for easier assertions
        tags_dict = {}
        for row in results:
            col = row.column_name
            if col not in tags_dict:
                tags_dict[col] = {}
            tags_dict[col][row.tag_name] = row.tag_value

        # Verify expected final state with "set only" behavior
        expected_tags = {
            "id": {"pii": "false", "source": "application"},  # source updated
            "msg": {"pii": "true"},  # new column tag
            "color": {"pii": "false"},  # persisted from original
        }
        assert tags_dict == expected_tags
