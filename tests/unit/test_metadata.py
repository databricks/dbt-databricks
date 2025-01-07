from unittest.mock import Mock, patch

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.databricks.behaviors.metadata import DefaultMetadataBehavior
from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksRelationType


class TestDefaultMetadataBehavior:
    @pytest.fixture
    def behavior(self):
        return DefaultMetadataBehavior()

    @pytest.fixture
    def relation(self):
        return BaseRelation.create("database", "schema")

    @pytest.fixture
    def adapter(self):
        a = Mock()
        a.Relation = DatabricksRelation
        return a

    def test_list_relations_without_caching__no_relations(self, behavior, relation):
        with patch.object(DefaultMetadataBehavior, "_get_relations_without_caching") as mocked:
            mocked.return_value = []
            assert behavior.list_relations_without_caching(Mock(), relation) == []

    def test_list_relations_without_caching__some_relations(self, behavior, relation, adapter):
        with patch.object(DefaultMetadataBehavior, "_get_relations_without_caching") as mocked:
            mocked.return_value = [("name", "table", "hudi", "owner")]
            relations = behavior.list_relations_without_caching(adapter, relation)
            assert len(relations) == 1
            rrelation = relations[0]
            assert rrelation.identifier == "name"
            assert rrelation.database == "database"
            assert rrelation.schema == "schema"
            assert rrelation.type == DatabricksRelationType.Table
            assert rrelation.owner == "owner"
            assert rrelation.is_hudi

    def test_list_relations_without_caching__hive_relation(self, behavior, relation, adapter):
        with patch.object(DefaultMetadataBehavior, "_get_relations_without_caching") as mocked:
            mocked.return_value = [("name", "table", None, None)]
            relations = behavior.list_relations_without_caching(adapter, relation)
            assert len(relations) == 1
            rrelation = relations[0]
            assert rrelation.identifier == "name"
            assert rrelation.database == "database"
            assert rrelation.schema == "schema"
            assert rrelation.type == DatabricksRelationType.Table
            assert not rrelation.has_information()
