import os

import pytest
from agate import Table
from dbt.adapters.databricks.impl import DatabricksAdapter
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.tests import util
from dbt.tests.adapter.persist_docs import fixtures
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
        view_comment = metadata["Comment"]

        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert view_comment == "" or view_comment is None

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
        return {"schema.yml": fixtures._PROPERITES__SCHEMA_MISSING_COL}

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


class TestPersistDocsWithSeeds:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        util.run_dbt(["seed"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": fixtures._SEEDS__SEED,
            "schema.yml": override_fixtures._SEEDS__SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def table_relation(self, project):
        return DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="seed",
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
