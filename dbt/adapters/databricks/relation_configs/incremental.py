from typing import Dict
from typing import Optional

from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationChangeSet
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationConfigBase
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor


class IncrementalTableConfig(DatabricksRelationConfigBase):
    config_components = [TagsProcessor]

    def get_changeset(
        self, existing: "IncrementalTableConfig"
    ) -> Optional[DatabricksRelationChangeSet]:
        changes: Dict[str, DatabricksComponentConfig] = {}

        for component in self.config_components:
            key = component.name
            value = self.config[key]
            diff = value.get_diff(existing.config[key])
            if diff:
                changes[key] = diff

        if len(changes) > 0:
            return DatabricksRelationChangeSet(changes=changes, requires_full_refresh=False)
        return None
