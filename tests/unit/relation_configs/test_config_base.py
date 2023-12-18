from dataclasses import dataclass
import pytest
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksConfigChange,
    DatabricksRelationChangeSet,
)
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.base import RelationConfigChangeAction
from dbt.adapters.databricks.relation_configs.refresh import (
    ManualRefreshConfig,
    ScheduledRefreshConfig,
)
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class MockComponentConfig(DatabricksComponentConfig):
    data: int = 1

    def to_sql_clause(self) -> str:
        return ""


class TestDatabricksComponentConfig:
    def test_get_diff__invalid_type(self):
        config = MockComponentConfig()
        other = CommentConfig()

        with pytest.raises(DbtRuntimeError, match="Cannot diff"):
            config.get_diff(other)

    def test_get_diff__valid_type(self):
        config = MockComponentConfig()
        other = MockComponentConfig(data=2)

        assert config.get_diff(other) == config


class TestDatabricksConfigChange:
    def test_requires_full_refresh__unalterable(self):
        change = DatabricksConfigChange(RelationConfigChangeAction.alter, MockComponentConfig())
        assert change.requires_full_refresh

    def test_requires_full_refresh__alterable(self):
        change = DatabricksConfigChange(RelationConfigChangeAction.alter, ManualRefreshConfig())
        assert not change.requires_full_refresh

    def test_get_change__no_change(self):
        change = DatabricksConfigChange.get_change(ManualRefreshConfig(), ManualRefreshConfig())
        assert change is None

    def test_get_change__with_diff(self):
        change = DatabricksConfigChange.get_change(
            ManualRefreshConfig(), ScheduledRefreshConfig(cron="*/5 * * * *", time_zone_value="UTC")
        )
        assert change == DatabricksConfigChange(
            RelationConfigChangeAction.alter, ManualRefreshConfig()
        )


class TestDatabricksRelationChangeSet:
    def test_requires_full_refresh__no_changes(self):
        changeset = DatabricksRelationChangeSet([])
        assert not changeset.requires_full_refresh

    def test_requires_full_refresh__has_only_alterable_changes(self):
        changeset = DatabricksRelationChangeSet(
            [DatabricksConfigChange(RelationConfigChangeAction.alter, ManualRefreshConfig())]
        )
        assert not changeset.requires_full_refresh

    def test_requires_full_refresh__has_an_inalterable_change(self):
        changeset = DatabricksRelationChangeSet(
            [
                DatabricksConfigChange(RelationConfigChangeAction.alter, ManualRefreshConfig()),
                DatabricksConfigChange(RelationConfigChangeAction.alter, CommentConfig()),
            ]
        )
        assert changeset.requires_full_refresh

    def test_has_changes__no_changes(self):
        changeset = DatabricksRelationChangeSet([])
        assert not changeset.has_changes

    def test_has_changes__has_changes(self):
        changeset = DatabricksRelationChangeSet(
            [DatabricksConfigChange(RelationConfigChangeAction.alter, ManualRefreshConfig())]
        )
        assert changeset.has_changes

    def test_get_alter_sql_clauses__no_changes(self):
        changeset = DatabricksRelationChangeSet([])
        assert changeset.get_alter_sql_clauses() == []

    def test_get_alter_sql_clauses__with_inalterable_change(self):
        changeset = DatabricksRelationChangeSet(
            [DatabricksConfigChange(RelationConfigChangeAction.alter, CommentConfig())]
        )
        with pytest.raises(AssertionError):
            changeset.get_alter_sql_clauses()

    def test_get_alter_sql_clauses__with_alterable_change(self):
        changeset = DatabricksRelationChangeSet(
            [
                DatabricksConfigChange(
                    RelationConfigChangeAction.alter,
                    TblPropertiesConfig(tblproperties={"key": "value"}, to_unset=["other"]),
                ),
                DatabricksConfigChange(RelationConfigChangeAction.alter, ManualRefreshConfig()),
            ]
        )
        assert changeset.get_alter_sql_clauses() == [
            "SET TBLPROPERTIES ('key' = 'value')",
            "UNSET TBLPROPERTIES IF EXISTS ('other')",
            "DROP SCHEDULE",
        ]
