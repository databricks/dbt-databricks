from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksRelationType
from tests.unit.macros.base import MacroTestBase


class TestApplyLiquidClusteredCols(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "liquid_clustering.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    @pytest.fixture
    def context(self, template) -> dict:
        # Render the body of `{% call statement(...) %}` blocks instead of the statement name.
        template.globals["statement"] = lambda name, caller: caller()
        return template.globals

    @pytest.fixture
    def target_relation(self):
        relation = Mock()
        relation.render = Mock(return_value="`db`.`schema`.`tbl`")
        relation.type = DatabricksRelationType.Table
        return relation

    def _auto_clustering(self):
        clustering = Mock()
        clustering.cluster_by = []
        clustering.auto_cluster = True
        return clustering

    def test_apply_auto_on_managed_table(self, template_bundle, target_relation):
        existing = Mock(spec=DatabricksRelation)
        existing.is_shallow_clone = False
        sql = self.run_macro(
            template_bundle.template,
            "apply_liquid_clustered_cols",
            target_relation,
            self._auto_clustering(),
            existing,
        )
        assert sql == "alter table `db`.`schema`.`tbl` cluster by auto"

    def test_apply_auto_skipped_on_shallow_clone(self, template_bundle, target_relation):
        existing = Mock(spec=DatabricksRelation)
        existing.is_shallow_clone = True
        sql = self.run_macro(
            template_bundle.template,
            "apply_liquid_clustered_cols",
            target_relation,
            self._auto_clustering(),
            existing,
        )
        assert "cluster by auto" not in sql
        template_bundle.context["exceptions"].warn.assert_called_once()
