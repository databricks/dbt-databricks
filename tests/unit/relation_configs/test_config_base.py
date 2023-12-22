from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksRelationChangeSet,
)

from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig


class MockComponentConfig(DatabricksComponentConfig):
    data: int = 1

    @property
    def requires_full_refresh(self) -> bool:
        return True


class TestDatabricksComponentConfig:
    def test_get_diff__valid_type(self):
        config = MockComponentConfig()
        other = MockComponentConfig(data=2)

        assert config.get_diff(other) == config


class TestDatabricksRelationChangeSet:
    def test_requires_full_refresh__no_changes(self):
        changeset = DatabricksRelationChangeSet(changes={})
        assert not changeset.requires_full_refresh

    def test_requires_full_refresh__has_only_alterable_changes(self):
        changeset = DatabricksRelationChangeSet(changes={"refresh": RefreshConfig()})
        assert not changeset.requires_full_refresh

    def test_requires_full_refresh__has_an_inalterable_change(self):
        changeset = DatabricksRelationChangeSet(
            changes={"comment": CommentConfig(), "refresh": RefreshConfig()}
        )
        assert changeset.requires_full_refresh

    def test_has_changes__no_changes(self):
        changeset = DatabricksRelationChangeSet(changes={})
        assert not changeset.has_changes

    def test_has_changes__has_changes(self):
        changeset = DatabricksRelationChangeSet(changes={"refresh": RefreshConfig()})
        assert changeset.has_changes
