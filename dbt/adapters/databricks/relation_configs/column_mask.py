from dataclasses import asdict
from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)
from dbt.adapters.relation_configs.config_base import RelationResults


class ColumnMaskConfig(DatabricksComponentConfig):
    # column name -> mask
    set_column_masks: dict[str, str]
    unset_column_masks: list[str] = []

    def get_diff(self, other: "ColumnMaskConfig") -> Optional["ColumnMaskConfig"]:
        # Find column masks that need to be unset
        unset_column_mask = [
            col for col in other.set_column_masks if col not in self.set_column_masks
        ]

        # Find column masks that need to be set or updated
        set_column_mask = {
            col: mask
            for col, mask in self.set_column_masks.items()
            if col not in other.set_column_masks or other.set_column_masks[col] != mask
        }

        if set_column_mask or unset_column_mask:
            return ColumnMaskConfig(
                set_column_masks=set_column_mask,
                unset_column_masks=unset_column_mask,
            )
        return None


class ColumnMaskProcessor(DatabricksComponentProcessor[ColumnMaskConfig]):
    name: ClassVar[str] = "column_masks"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ColumnMaskConfig:
        column_masks = results.get("column_masks")
        set_column_masks = {}

        if column_masks:
            for row in column_masks.rows:
                set_column_masks[row[0]] = row[1]

        return ColumnMaskConfig(set_column_masks=set_column_masks)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> ColumnMaskConfig:
        # Extract config from model node
        columns = getattr(relation_config, "columns", {})
        columns = [
            {"name": name, **(col if isinstance(col, dict) else asdict(col))}
            for name, col in columns.items()
        ]

        set_column_masks = {}
        for col in columns:
            extra = col.get("_extra", {})
            if extra and "column_mask" in extra:
                set_column_masks[col["name"]] = extra["column_mask"]
        return ColumnMaskConfig(set_column_masks=set_column_masks)
