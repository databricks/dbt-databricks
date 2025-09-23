from typing import Optional

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesProcessor


class IncrementalTableConfig(DatabricksRelationConfigBase):
    config_components = [TagsProcessor, TblPropertiesProcessor]

    def get_changeset(
        self, existing: "IncrementalTableConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        changes: dict[str, DatabricksComponentConfig] = {}

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if diff:
                changes[key] = diff

        if len(changes) > 0:
            return DatabricksRelationChangeSet(changes=changes, requires_full_refresh=False)
        return None
