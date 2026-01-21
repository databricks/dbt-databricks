from typing import ClassVar, Optional

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.base import (
    DatabricksComponentConfig,
    DatabricksComponentProcessor,
    DatabricksRelationConfigBase,
)
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesProcessor


class MetricViewQueryConfig(DatabricksComponentConfig):
    """Component encapsulating the YAML definition of a metric view."""

    query: str

    def get_diff(self, other: "MetricViewQueryConfig") -> Optional["MetricViewQueryConfig"]:
        # Normalize whitespace for comparison
        self_normalized = " ".join(self.query.split())
        other_normalized = " ".join(other.query.split())
        if self_normalized != other_normalized:
            return self
        return None


class MetricViewQueryProcessor(DatabricksComponentProcessor[MetricViewQueryConfig]):
    """Processor for metric view YAML definitions.

    Metric views store their YAML definitions in information_schema.views, but wrapped
    in $$ delimiters. This processor extracts and compares the YAML content.
    """

    name: ClassVar[str] = "query"

    @classmethod
    def from_relation_results(cls, result: RelationResults) -> MetricViewQueryConfig:
        from dbt.adapters.databricks.logging import logger

        # Get the view text from DESCRIBE EXTENDED output
        describe_extended = result.get("describe_extended")
        if not describe_extended:
            raise DbtRuntimeError(
                f"Cannot find metric view description. Result keys: {list(result.keys())}"
            )

        # Find the "View Text" row in DESCRIBE EXTENDED output
        view_definition = None
        for row in describe_extended:
            if row[0] == "View Text":
                view_definition = row[1]
                break

        logger.debug(
            f"MetricViewQueryProcessor: view_definition = "
            f"{view_definition[:200] if view_definition else 'None'}"
        )

        if not view_definition:
            raise DbtRuntimeError("Metric view has no 'View Text' in DESCRIBE EXTENDED output")

        view_definition = view_definition.strip()

        # Extract YAML content from $$ delimiters if present
        # Format: $$ yaml_content $$
        if "$$" in view_definition:
            parts = view_definition.split("$$")
            if len(parts) >= 2:
                # The YAML is between the first and second $$ markers
                view_definition = parts[1].strip()

        return MetricViewQueryConfig(query=view_definition)

    @classmethod
    def from_relation_config(cls, relation_config: RelationConfig) -> MetricViewQueryConfig:
        query = relation_config.compiled_code

        if query:
            return MetricViewQueryConfig(query=query.strip())
        else:
            raise DbtRuntimeError(
                f"Cannot compile metric view {relation_config.identifier} with no YAML definition"
            )


class MetricViewConfig(DatabricksRelationConfigBase):
    """Config for metric views.

    Metric views use YAML definitions stored in information_schema.views wrapped in $$ delimiters.
    Changes to the YAML definition can be applied via ALTER VIEW AS.
    Tags and tblproperties can also be altered incrementally.
    """

    config_components = [
        TagsProcessor,
        TblPropertiesProcessor,
        MetricViewQueryProcessor,
    ]
