from dataclasses import dataclass
from typing import Any, ClassVar
from typing import Optional

from dbt.adapters.spark.column import SparkColumn
from dbt_common.contracts.constraints import ColumnLevelConstraint
from dbt_common.contracts.constraints import ConstraintType
from dbt.adapters.databricks import constraints


@dataclass
class DatabricksColumn(SparkColumn):
    table_comment: Optional[str] = None
    comment: Optional[str] = None
    not_null: Optional[bool] = None
    constraints: Optional[list[ColumnLevelConstraint]] = None

    TYPE_LABELS: ClassVar[dict[str, str]] = {
        "LONG": "BIGINT",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return super(SparkColumn, cls).translate_type(dtype).lower()

    @classmethod
    def create(cls, name: str, label_or_dtype: str) -> "DatabricksColumn":
        column_type = cls.translate_type(label_or_dtype)
        return cls(name, column_type)

    @property
    def data_type(self) -> str:
        return self.translate_type(self.dtype)

    def add_constraint(self, constraint: ColumnLevelConstraint) -> None:
        # On first constraint add, initialize constraint details
        if self.constraints is None:
            self.constraints = []
            self.not_null = False
        if constraint.type == ConstraintType.not_null:
            self.not_null = True
        else:
            self.constraints.append(constraint)

    def enrich(self, model_column: dict[str, Any]) -> "DatabricksColumn":
        """Create a copy that incorporates model column metadata, including constraints."""

        data_type = model_column.get("data_type") or self.dtype
        enriched_column = DatabricksColumn.create(self.name, data_type)
        if model_column.get("description"):
            enriched_column.comment = model_column["description"]

        if model_column.get("constraints"):
            for constraint in model_column["constraints"]:
                parsed_constraint = constraints.parse_column_constraint(constraint)
                enriched_column.add_constraint(parsed_constraint)

        return enriched_column

    def render_for_create(self) -> str:
        """Renders the column for building a create statement."""
        column_str = f"{self.name} {self.dtype}"
        if self.not_null:
            column_str += " NOT NULL"
        if self.comment:
            comment = self.comment.replace("'", "\\'")
            column_str += f" COMMENT '{comment}'"
        for constraint in self.constraints or []:
            c = constraints.process_column_constraint(constraint)
            if c != "":
                column_str += f" {c}"
        return column_str

    def __repr__(self) -> str:
        return "<DatabricksColumn {} ({})>".format(self.name, self.data_type)
