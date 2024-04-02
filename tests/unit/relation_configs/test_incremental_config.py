from agate import Table

from dbt.adapters.databricks.relation_configs.incremental import IncrementalTableConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig


class TestIncrementalConfig:
    def test_from_results(self):
        results = {
            "information_schema.tags": {
                Table(
                    rows=[
                        ["tag1", "value1"],
                        ["tag2", "value2"],
                    ],
                    column_names=["tag_name", "tag_value"],
                )
            }
        }

        config = IncrementalTableConfig.from_results(results)

        assert config == IncrementalTableConfig(
            config={
                "tags": TagsConfig(set_tags={"tag1": "value1", "tag2": "value2"}),
            }
        )
