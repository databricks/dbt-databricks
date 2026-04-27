from typing import Optional

from typing_extensions import Self

from dbt.adapters.databricks.logging import logger
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksRelationChangeSet,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsProcessor
from dbt.adapters.databricks.relation_configs.comment import CommentProcessor
from dbt.adapters.databricks.relation_configs.query import ViewQueryProcessor
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesProcessor


class ViewConfig(DatabricksRelationConfigBase):
    config_components = [
        TagsProcessor,
        TblPropertiesProcessor,
        ViewQueryProcessor,
        CommentProcessor,
        ColumnTagsProcessor,
    ]

    def get_changeset(self, existing: Self) -> DatabricksRelationChangeSet:
        # view query processor will always return a change
        changeset = super().get_changeset(existing)
        if "comment" in changeset.changes:
            logger.debug(
                "View description changed, requiring replace, as there is"
                " no API yet to update comments."
            )
            changeset.requires_full_refresh = True
        return changeset
