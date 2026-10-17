from unittest.mock import Mock

import pytest
from dbt_common.exceptions import CompilationError
from dbt_common.exceptions.macros import MacroReturn

from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from tests.unit.macros.base import MacroTestBase

LOCATION_KEY = "delta.universalFormat.compatibility.location"
LOCATION = "abfss://c@a.dfs.core.windows.net/mv"


def rows(*values):
    return Mock(rows=[tuple(v) for v in values])


class StrategyMacroTestBase(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace_strategy.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/materialized_view", "macros/relations", "macros"]

    @pytest.fixture
    def default_context(self) -> dict:
        context = MacroTestBase.default_context.__wrapped__(self)

        def macro_return(value):
            raise MacroReturn(value)

        def raise_compiler_error(message):
            raise CompilationError(message)

        context["return"] = macro_return
        context["exceptions"].raise_compiler_error = raise_compiler_error
        return context

    def call(self, template, name, *args):
        with pytest.raises(MacroReturn) as result:
            self.run_macro_raw(template, name, *args)
        return result.value.value


class TestMvCompatibilityRebuildStrategy(StrategyMacroTestBase):
    def decide(self, template, desired_location, existing_location, desired_parts, existing_parts):
        return self.call(
            template,
            "mv_compatibility_rebuild_strategy",
            desired_location,
            existing_location,
            desired_parts,
            existing_parts,
        )

    @pytest.mark.parametrize(
        "existing_location, desired_parts, existing_parts, strategy",
        [
            (LOCATION, [], [], "replace"),
            (LOCATION, ["`ID`"], ["id"], "replace"),
            (None, [], [], "replace"),
            (None, ["id"], ["id"], "replace"),
            (LOCATION, [], ["id"], "refuse"),
            (LOCATION, ["name"], ["id"], "refuse"),
            (LOCATION, ["id"], [], "refuse"),
            (None, ["name"], ["id"], "refuse"),
            (None, [], ["id"], "refuse"),
        ],
    )
    def test_partition_and_location_combinations(
        self, template, existing_location, desired_parts, existing_parts, strategy
    ):
        decision = self.decide(template, LOCATION, existing_location, desired_parts, existing_parts)

        assert decision["strategy"] == strategy
        if strategy == "refuse":
            assert "changing partition_by" in decision["reason"]
            assert LOCATION in decision["reason"]

    @pytest.mark.parametrize("desired_parts", [[], ["id"]])
    def test_location_change_refuses(self, template, desired_parts):
        decision = self.decide(template, LOCATION + "_2", LOCATION, desired_parts, ["id"])

        assert decision["strategy"] == "refuse"
        assert f"from '{LOCATION}' to '{LOCATION}_2'" in decision["reason"]

    @pytest.mark.parametrize(
        "desired_location, existing_location, side",
        [("", LOCATION, "desired"), ("  ", None, "desired"), (LOCATION, " ", "existing")],
    )
    def test_empty_location_refuses(self, template, desired_location, existing_location, side):
        decision = self.decide(template, desired_location, existing_location, [], [])

        assert decision["strategy"] == "refuse"
        assert f"the {side} '{LOCATION_KEY}' is set but empty" in decision["reason"]


class TestGetMvCompatibilityRebuildSql(StrategyMacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(autouse=True)
    def desired(self, context):
        def configure(location, parts=()):
            context["adapter"].get_config_from_model.return_value.config = {
                "partition_by": PartitionedByConfig(partition_by=list(parts)),
                "tblproperties": TblPropertiesConfig(
                    tblproperties={} if location is None else {LOCATION_KEY: location}
                ),
            }

        configure(LOCATION)
        context["fetch_tbl_properties"] = Mock(
            return_value=rows((LOCATION_KEY, LOCATION), ("pipelines.pipelineId", "p1"))
        )
        context["describe_table_extended_without_caching"] = Mock(
            return_value=rows(
                ("id", "bigint", None),
                ("grp", "string", None),
                ("# Partition Information", "", ""),
                ("# col_name", "data_type", "comment"),
                ("grp", "string", None),
                ("", "", ""),
                ("# Detailed Table Information", "", ""),
            )
        )
        context["mv_compatibility_rebuild_strategy"] = Mock(return_value={"strategy": "replace"})
        context["get_replace_materialized_view_sql"] = Mock(return_value="create or replace mv")
        return configure

    def rebuild(self, template_bundle):
        return self.call(
            template_bundle.template,
            "get_mv_compatibility_rebuild_sql",
            Mock(),
            template_bundle.relation,
            "select 1",
        )

    def test_without_desired_location_reads_no_metadata(self, template_bundle, context, desired):
        desired(None)

        assert self.rebuild(template_bundle) is None
        context["fetch_tbl_properties"].assert_not_called()
        context["describe_table_extended_without_caching"].assert_not_called()
        context["adapter"].get_relation_config.assert_not_called()

    def test_reads_existing_location_and_partitions(self, template_bundle, context, desired):
        desired(LOCATION, ["grp"])

        assert self.rebuild(template_bundle) == "create or replace mv"
        context["mv_compatibility_rebuild_strategy"].assert_called_once_with(
            LOCATION, LOCATION, ["grp"], ["grp"]
        )
        context["adapter"].get_relation_config.assert_not_called()

    def test_refuse_raises_before_any_drop(self, template_bundle, context):
        context["mv_compatibility_rebuild_strategy"].return_value = {
            "strategy": "refuse",
            "reason": "location would be reused",
        }

        with pytest.raises(CompilationError, match="location would be reused"):
            self.run_macro_raw(
                template_bundle.template,
                "get_mv_compatibility_rebuild_sql",
                Mock(),
                template_bundle.relation,
                "select 1",
            )
        context["get_replace_materialized_view_sql"].assert_not_called()
