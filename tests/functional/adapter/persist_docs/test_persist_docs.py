import json
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)
from dbt.tests import util

import pytest


class TestPersistDocs(BasePersistDocsBase):
    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_comment = view_node["metadata"]["comment"]
        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert view_comment == ""

        view_id_comment = view_node["columns"]["id"]["comment"]
        if has_column_comments:
            assert view_id_comment.startswith("id Column description")
            self._assert_common_comments(view_id_comment)
        else:
            assert view_id_comment is None

        view_name_comment = view_node["columns"]["name"]["comment"]
        assert view_name_comment is None

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

    def test_has_comments(self, project):
        util.run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 4
        table_node = catalog_data["nodes"]["model.test.table_model"]
        self._assert_has_table_comments(table_node)

        view_node = catalog_data["nodes"]["model.test.view_model"]
        self._assert_has_view_comments(view_node)

        no_docs_node = catalog_data["nodes"]["model.test.no_docs_model"]
        self._assert_has_view_comments(no_docs_node, False, False)


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
