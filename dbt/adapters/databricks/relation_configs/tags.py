from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs import base
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentConfig
from dbt.adapters.databricks.relation_configs.base import DatabricksComponentProcessor
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError


class TagsConfig(DatabricksComponentConfig):
    """Component encapsulating the tblproperties of a relation."""

    set_tags: Dict[str, str]
    unset_tags: List[str] = []

    def get_diff(self, other: "TagsConfig") -> Optional["TagsConfig"]:
        to_unset = []
        for k in other.set_tags.keys():
            if k not in self.set_tags:
                to_unset.append(k)
        return TagsConfig(set_tags=self.set_tags, unset_tags=to_unset)


class TagsProcessor(DatabricksComponentProcessor[TagsConfig]):
    name: ClassVar[str] = "tags"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> TagsConfig:
        table = results.get("information_schema.tags")
        tags = dict()

        if table:
            for row in table.rows:
                tags[str(row[0])] = str(row[1])

        return TagsConfig(set_tags=tags)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> TagsConfig:
        tags = base.get_config_value(relation_config, "databricks_tags")
        if not tags:
            return TagsConfig(set_tags=dict())
        if isinstance(tags, Dict):
            tags = {str(k): str(v) for k, v in tags.items()}
            return TagsConfig(set_tags=tags)
        else:
            raise DbtRuntimeError("databricks_tags must be a dictionary")
