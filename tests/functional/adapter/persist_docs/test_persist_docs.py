import os

import pytest
from agate import Table
from dbt.tests import util
from dbt.tests.adapter.persist_docs import fixtures

from dbt.adapters.databricks.impl import DatabricksAdapter
from dbt.adapters.databricks.relation import DatabricksRelation
from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.persist_docs import fixtures as override_fixtures


class DatabricksPersistDocsMixin:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt(["seed"])
        util.run_dbt()

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures._SEEDS__SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_docs_model.sql": fixtures._MODELS__NO_DOCS_MODEL,
            "table_model.sql": fixtures._MODELS__TABLE,
            "view_model.sql": fixtures._MODELS__VIEW,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "my_fun_docs.md": fixtures._DOCS__MY_FUN_DOCS,
            "schema.yml": fixtures._PROPERTIES__SCHEMA_YML,
        }

    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert '"with double quotes"' in comment
            assert """'''abc123'''""" in comment
            assert "\n" in comment
            assert "Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting" in comment
            assert "/* comment */" in comment
            if os.name == "nt":
                assert "--\r\n" in comment or "--\n" in comment
            else:
                assert "--\n" in comment

    def _assert_has_view_comments(
        self,
        adapter: DatabricksAdapter,
        relation: DatabricksRelation,
        has_node_comments: bool = True,
        has_column_comments: bool = True,
    ):
        results = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        metadata, columns = adapter.parse_describe_extended(
            relation, Table(results, ["col_name", "data_type", "comment"])
        )

        if has_node_comments:
            view_comment = metadata["Comment"]
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert metadata.get("Comment") is None

        for column in columns:
            if column.column == "id":
                view_id_comment = column.comment
                if has_column_comments:
                    assert view_id_comment and view_id_comment.startswith("id Column description")
                    self._assert_common_comments(view_id_comment)
                else:
                    assert view_id_comment is None

            if column.column == "name":
                view_name_comment = column.comment
                assert view_name_comment is None

    def _assert_has_table_comments(self, adapter: DatabricksAdapter, relation: DatabricksRelation):
        results = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        metadata, columns = adapter.parse_describe_extended(
            relation, Table(results, ["col_name", "data_type", "comment"])
        )
        table_comment = metadata["Comment"]
        assert table_comment.startswith("Table model description")

        for column in columns:
            if column.column == "id":
                table_id_comment = column.comment
                assert table_id_comment and table_id_comment.startswith("id Column description")
                self._assert_common_comments(table_id_comment)

            if column.column == "name":
                table_name_comment = column.comment
                assert table_name_comment and table_name_comment.startswith(
                    "Some stuff here and then a call to"
                )

        self._assert_common_comments(table_comment, table_id_comment, table_name_comment)


class TestPersistDocs(DatabricksPersistDocsMixin):
    @pytest.fixture(scope="class")
    def view_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="view_model",
            type="view",
        )

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="table_model",
            type="table",
        )

    @pytest.fixture(scope="class")
    def no_docs_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="no_docs_model",
            type="view",
        )

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_has_comments(self, adapter, view_relation, table_relation, no_docs_relation):
        self._assert_has_view_comments(adapter, view_relation)
        self._assert_has_table_comments(adapter, table_relation)
        self._assert_has_view_comments(adapter, no_docs_relation, False, False)


class TestPersistDocsColumnMissing(DatabricksPersistDocsMixin):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column.sql": fixtures._MODELS__MISSING_COLUMN}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures._PROPERTIES__SCHEMA_MISSING_COL}

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="missing_column",
            type="table",
        )

    def test_missing_column(self, adapter, table_relation):
        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )
        for column in columns:
            if column.column == "id":
                table_id_comment = column.comment
                assert table_id_comment and table_id_comment.startswith(
                    "test id column description"
                )
                break


class TestPersistDocsCommentOnQuotedColumn:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt()

    @pytest.fixture(scope="class")
    def models(self):
        return {"quote_model.sql": fixtures._MODELS__MODEL_USING_QUOTE_UTIL}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"properties.yml": fixtures._PROPERTIES__QUOTE_MODEL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="quote_model",
            type="table",
        )

    def test_quoted_column_comments(self, adapter, table_relation):
        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )

        for column in columns:
            if column.column == "2id":
                column_comment = column.comment
                assert column_comment.startswith("XXX")
                break


@pytest.mark.external
# Skipping UC Cluster to ensure these tests don't fail due to overlapping resources
@pytest.mark.skip_profile("databricks_uc_cluster")
class TestPersistDocsWithSeeds:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt(["seed"])

    @pytest.fixture(scope="class")
    def seeds(self, adapter):
        if (
            adapter.config.credentials.database
            and adapter.config.credentials.database != "hive_metastore"
        ):
            return {
                "persist_seed.csv": fixtures._SEEDS__SEED,
                "schema.yml": override_fixtures._SEEDS__SCHEMA_YML,
            }
        else:
            return {
                "persist_seed.csv": fixtures._SEEDS__SEED,
                "schema.yml": override_fixtures._HIVE__SCHEMA_YML,
            }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="persist_seed",
            type="table",
        )

    def test_reseeding_with_persist_docs(self, table_relation, adapter):
        util.run_dbt(["seed"])
        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        metadata, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )
        table_comment = metadata["Comment"]
        assert table_comment.startswith("A seed description")
        assert columns[0].comment.startswith("An id column")
        assert columns[1].comment.startswith("A name column")


@pytest.mark.external
# Skipping UC Cluster to ensure these tests don't fail due to overlapping resources
@pytest.mark.skip_profile("databricks_uc_cluster")
class TestPersistDocsWithSeedsV2(TestPersistDocsWithSeeds, MaterializationV2Mixin):
    pass


class TestPersistDocsCaseMismatch:
    """Test for issue #1215: case mismatch between model columns and YAML schema."""

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt(["run"])

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "case_mismatch_model.sql": """
                {{ config(materialized='table') }}
                select 1 as Account_ID, 'test' as User_Name
            """
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "schema.yml": """
version: 2
models:
  - name: case_mismatch_model
    description: 'Model with case mismatch in column names'
    columns:
      - name: account_id
        description: 'Account ID column'
      - name: user_name
        description: 'User name column'
            """
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="case_mismatch_model",
            type="table",
        )

    def test_case_mismatch_persist_docs(self, adapter, table_relation):
        """Test that persist_docs handles case mismatches gracefully."""
        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )

        # Verify that comments were persisted despite case mismatch
        for column in columns:
            if column.column.lower() == "account_id":
                assert column.comment and column.comment.startswith("Account ID column")
            elif column.column.lower() == "user_name":
                assert column.comment and column.comment.startswith("User name column")


class TestPersistDocsColumnsGateV1:
    """v1 gates inline column COMMENT on persist_docs.columns.

    With {relation: true, columns: false} a v1 model with a column description gets no
    column comment, where the typed create path emits it unconditionally.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt(["run"])

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "gate_model.sql": override_fixtures.gate_model_sql,
            "schema.yml": override_fixtures.gate_model_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {"test": {"+persist_docs": {"relation": True, "columns": False}}},
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="gate_model",
            type="table",
        )

    def test_column_comment_suppressed_when_columns_false(self, adapter, table_relation):
        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )
        id_columns = [c for c in columns if c.column == "id"]
        assert id_columns, "id column missing from describe output"
        assert not id_columns[0].comment, (
            f"v1 must suppress column comment when persist_docs.columns is false, "
            f"got {id_columns[0].comment!r}"
        )


_JINJA_WARNING_ERROR_OPTIONS = '{"error": ["JinjaLogWarning"]}'

# Substring of the warning dbt_databricks_validate_doc_columns emits for a documented column
# absent from the relation ("In relation <rel>: The following columns are specified in the
# schema but are not present in the database: <col>").
_MISSING_COLUMN_WARNING = "are not present in the database"


class TestPersistDocsColumnMissingWarnsV1:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column.sql": fixtures._MODELS__MISSING_COLUMN}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures._PROPERTIES__SCHEMA_MISSING_COL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {"test": {"+persist_docs": {"relation": True, "columns": True}}},
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="missing_column",
            type="table",
        )

    def test_warns_and_still_comments_present_columns(self, adapter, table_relation):
        util.run_dbt(["run"])

        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )
        id_columns = [c for c in columns if c.column == "id"]
        assert id_columns and id_columns[0].comment
        assert id_columns[0].comment.startswith("test id column description")

        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )


class TestPersistDocsColumnMissingWarnsV2:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_incremental.sql": override_fixtures.missing_column_incremental_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_incremental_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": True, "columns": True}}},
        }

    def test_warning_escalates_on_create_and_subsequent_runs(self, project, adapter):
        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )

        relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="missing_column_incremental",
            type="table",
        )
        rows = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        _, columns = adapter.parse_describe_extended(
            relation, Table(rows, ["col_name", "data_type", "comment"])
        )
        comments = {column.column: column.comment for column in columns}
        assert comments["id"] == "test id column description"

        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )


class TestPersistDocsColumnMissingWarnsV2ColumnsOnly:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_incremental.sql": override_fixtures.missing_column_incremental_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_incremental_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    def test_warning_escalates_with_columns_only(self, project, adapter):
        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )

        relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="missing_column_incremental",
            type="table",
        )
        rows = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        _, columns = adapter.parse_describe_extended(
            relation, Table(rows, ["col_name", "data_type", "comment"])
        )
        comments = {column.column: column.comment for column in columns}
        assert comments["id"] == "test id column description"


class TestPersistDocsColumnMissingV2RelationOnlyNoWarn:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_incremental.sql": override_fixtures.missing_column_incremental_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_incremental_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": True, "columns": False}}},
        }

    def test_no_warning_when_columns_disabled(self, project):
        util.run_dbt(["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS])


class TestPersistDocsColumnMissingWarnsV1ColumnsOnly:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column.sql": fixtures._MODELS__MISSING_COLUMN}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures._PROPERTIES__SCHEMA_MISSING_COL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="missing_column",
            type="table",
        )

    def test_warning_escalates_with_columns_only(self, adapter, table_relation):
        util.run_dbt(["run"])

        results = util.run_sql_with_adapter(
            adapter, f"describe extended {table_relation}", fetch="all"
        )
        _, columns = adapter.parse_describe_extended(
            table_relation, Table(results, ["col_name", "data_type", "comment"])
        )
        id_columns = [c for c in columns if c.column == "id"]
        assert id_columns and id_columns[0].comment
        assert id_columns[0].comment.startswith("test id column description")

        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )


class TestPersistDocsColumnMissingWarnsV1IncrementalSubsequent:
    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_incremental.sql": override_fixtures.missing_column_incremental_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_incremental_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {
                "test": {
                    "+persist_docs": {"relation": True, "columns": True},
                    "+incremental_apply_config_changes": True,
                }
            },
        }

    def test_warning_escalates_on_subsequent_run(self, project):
        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )
        util.run_dbt(
            ["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS], expect_pass=False
        )


class TestPersistDocsPlannedColumnV1Incremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_change_incremental.sql": (
                override_fixtures.schema_change_incremental_initial_sql
            )
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.schema_change_incremental_initial_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {
                "test": {
                    "+persist_docs": {"relation": False, "columns": True},
                }
            },
        }

    def test_new_documented_column_is_not_warned_before_schema_sync(self, project, adapter):
        util.run_dbt(["run"])
        util.write_file(
            override_fixtures.schema_change_incremental_updated_sql,
            project.project_root,
            "models",
            "schema_change_incremental.sql",
        )
        util.write_file(
            override_fixtures.schema_change_incremental_updated_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

        util.run_dbt(["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS])

        relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="schema_change_incremental",
            type="table",
        )
        rows = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        _, columns = adapter.parse_describe_extended(
            relation, Table(rows, ["col_name", "data_type", "comment"])
        )
        comments = {column.column: column.comment for column in columns}
        assert comments["new_col"] == "new column comment"


@pytest.mark.skip_profile("databricks_cluster")
class TestPersistDocsPlannedColumnV2AlterView:
    @pytest.fixture(scope="class")
    def models(self):
        return {"alter_view.sql": override_fixtures.alter_view_initial_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.alter_view_initial_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "test": {
                    "+persist_docs": {"relation": False, "columns": True},
                }
            },
        }

    def test_new_documented_column_is_not_warned_before_alter_view(self, project, adapter):
        util.run_dbt(["run"])
        util.write_file(
            override_fixtures.alter_view_updated_sql,
            project.project_root,
            "models",
            "alter_view.sql",
        )
        util.write_file(
            override_fixtures.alter_view_updated_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

        util.run_dbt(["run", "--warn-error-options", _JINJA_WARNING_ERROR_OPTIONS])

        relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="alter_view",
            type="view",
        )
        rows = util.run_sql_with_adapter(adapter, f"describe extended {relation}", fetch="all")
        _, columns = adapter.parse_describe_extended(
            relation, Table(rows, ["col_name", "data_type", "comment"])
        )
        comments = {column.column: column.comment for column in columns}
        assert comments["id"] == "updated id comment"
        assert comments["added_col"] == "added column comment"


class TestPersistDocsColumnMissingWarnsViewCreate:
    """v2 view create: a documented column absent from the view is warned about post-build."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mc_seed.csv": override_fixtures.missing_column_create_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_view.sql": override_fixtures.missing_column_view_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_view_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    def test_view_create_warns(self, project):
        util.run_dbt(["seed"])
        _, logs = util.run_dbt_and_capture(["run"])
        assert "column_that_does_not_exist" in logs
        assert logs.count(_MISSING_COLUMN_WARNING) == 1


class TestPersistDocsColumnMissingWarnsMaterializedViewCreate:
    """materialized view create: a documented column absent from the MV warns post-build."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mc_seed.csv": override_fixtures.missing_column_create_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_mv.sql": override_fixtures.missing_column_mv_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_mv_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    def test_materialized_view_create_warns(self, project):
        util.run_dbt(["seed"])
        _, logs = util.run_dbt_and_capture(["run"])
        assert "column_that_does_not_exist" in logs
        assert logs.count(_MISSING_COLUMN_WARNING) == 1


class TestPersistDocsColumnMissingWarnsStreamingTableCreate:
    """streaming table create: a documented column absent from the ST is warned about post-build."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mc_seed.csv": override_fixtures.missing_column_create_seed}

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column_st.sql": override_fixtures.missing_column_st_sql}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": override_fixtures.missing_column_st_schema}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    def test_streaming_table_create_warns(self, project):
        util.run_dbt(["seed"])
        _, logs = util.run_dbt_and_capture(["run"])
        assert "column_that_does_not_exist" in logs
        assert logs.count(_MISSING_COLUMN_WARNING) == 1
