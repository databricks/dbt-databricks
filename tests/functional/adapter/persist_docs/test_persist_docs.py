import pytest

from dbt.tests.adapter.persist_docs.test_persist_docs import BasePersistDocs
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsColumnMissing,
)
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsCommentOnQuotedColumn,
)


class DatabricksPersistMixin:
    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_comment = view_node["metadata"]["comment"]
        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert view_comment == "" or view_comment is None

        view_id_comment = view_node["columns"]["id"]["comment"]
        if has_column_comments:
            assert view_id_comment.startswith("id Column description")
            self._assert_common_comments(view_id_comment)
        else:
            assert view_id_comment is None

        view_name_comment = view_node["columns"]["name"]["comment"]
        assert view_name_comment is None


@pytest.mark.skip_profile("databricks_cluster")
class TestPersistDocsUC(DatabricksPersistMixin, BasePersistDocs):
    pass


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestPersistDocsHMS(BasePersistDocs):
    def _assert_has_table_comments(self, table_node):
        table_comment = table_node["metadata"]["comment"]
        assert table_comment.startswith("Table model description")

        self._assert_common_comments(table_comment)

    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_comment = view_node["metadata"]["comment"]
        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert view_comment is None


@pytest.mark.skip_profile("databricks_cluster")
class TestPersistDocsUCColumnMissing(
    DatabricksPersistMixin, BasePersistDocsColumnMissing
):
    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestPersistDocsUCCommentOnQuotedColumn(
    DatabricksPersistMixin, BasePersistDocsCommentOnQuotedColumn
):
    pass
