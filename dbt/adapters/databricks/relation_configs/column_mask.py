from dataclasses import asdict
from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
)


class ColumnMaskConfig(DatabricksComponentConfig):
    # column name -> mask config (function name and optional using_columns)
    set_column_masks: dict[str, dict[str, str]]

    def get_diff(self, other: "ColumnMaskConfig") -> Optional["ColumnMaskConfig"]:
        # Add-only: masks the model doesn't declare may be owned by other tools, so they stay.
        # Column names compare case-insensitively; mask values compare exactly.
        existing_masks = {col.lower(): mask for col, mask in other.set_column_masks.items()}
        set_column_masks = {
            col: mask
            for col, mask in self.set_column_masks.items()
            if existing_masks.get(col.lower()) != mask
        }

        if set_column_masks:
            return ColumnMaskConfig(set_column_masks=set_column_masks)
        return None


class ColumnMaskProcessor(DatabricksComponentProcessor[ColumnMaskConfig]):
    name: ClassVar[str] = "column_masks"

    @classmethod
    def from_relation_results(cls, results: RelationResults) -> ColumnMaskConfig:
        column_masks = results.get("column_masks")
        set_column_masks = {}

        if column_masks:
            for row in column_masks.rows:
                # row contains [column_name, mask_name, using_columns]
                mask_config = {"function": row[1]}
                if row[2]:
                    mask_config["using_columns"] = row[2]
                set_column_masks[row[0]] = mask_config

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
            column_mask = extra.get("column_mask") if extra else None
            if column_mask:
                fully_qualified_function_name = (
                    column_mask["function"]
                    if "." in column_mask["function"]
                    else (
                        f"`{relation_config.database}`."
                        f"`{relation_config.schema}`."
                        f"`{column_mask['function']}`"
                    )
                )
                column_mask["function"] = fully_qualified_function_name
                set_column_masks[col["name"]] = column_mask
        return ColumnMaskConfig(set_column_masks=set_column_masks)
