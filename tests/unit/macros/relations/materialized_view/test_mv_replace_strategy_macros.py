from unittest.mock import Mock

import pytest
from dbt_common.exceptions import CompilationError
from dbt_common.exceptions.macros import MacroReturn

from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from tests.unit.macros.base import MacroTestBase

LOCATION_KEY = "delta.universalFormat.compatibility.location"
LOCATION = "abfss://c@a.dfs.core.windows.net/mv"


def props(location=None, **extra):
    properties = dict(extra)
    if location is not None:
        properties[LOCATION_KEY] = location
    return properties


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
    def decide(self, template, desired_parts, existing_parts, desired_props, existing_props):
        return self.call(
            template,
            "mv_compatibility_rebuild_strategy",
            desired_parts,
            existing_parts,
            desired_props,
            existing_props,
        )

    @pytest.mark.parametrize(
        "desired_parts, existing_parts",
        [([], []), (["id"], []), ([], ["id"]), (["id"], ["name"])],
    )
    def test_no_location_keeps_existing_behavior(self, template, desired_parts, existing_parts):
        assert self.decide(template, desired_parts, existing_parts, props(), props()) is None

    @pytest.mark.parametrize(
        "desired_parts, existing_parts, desired_location, existing_location, strategy",
        [
            ([], [], LOCATION, LOCATION, "replace"),
            (["`ID`"], ["id"], LOCATION, LOCATION, "replace"),
            ([], [], LOCATION, None, "replace"),
            (["id"], ["id"], None, LOCATION, "replace"),
            ([], ["id"], None, LOCATION, "drop"),
            (["name"], ["id"], None, LOCATION, "drop"),
            ([], ["id"], LOCATION, LOCATION, "refuse"),
            (["name"], ["id"], LOCATION, LOCATION, "refuse"),
            (["id"], [], LOCATION, LOCATION, "refuse"),
            (["name"], ["id"], LOCATION, None, "refuse"),
        ],
    )
    def test_partition_and_location_combinations(
        self,
        template,
        desired_parts,
        existing_parts,
        desired_location,
        existing_location,
        strategy,
    ):
        decision = self.decide(
            template,
            desired_parts,
            existing_parts,
            props(desired_location),
            props(existing_location, **{"pipelines.other": "x"}),
        )

        assert decision["strategy"] == strategy
        if strategy == "refuse":
            assert "changing partition_by" in decision["reason"]
            assert LOCATION in decision["reason"]

    @pytest.mark.parametrize("desired_parts", [[], ["id"]])
    def test_location_change_refuses(self, template, desired_parts):
        decision = self.decide(
            template, desired_parts, ["id"], props(LOCATION + "_2"), props(LOCATION)
        )

        assert decision["strategy"] == "refuse"
        assert f"from '{LOCATION}' to '{LOCATION}_2'" in decision["reason"]

    @pytest.mark.parametrize(
        "desired_location, existing_location, side",
        [("", LOCATION, "desired"), ("  ", None, "desired"), (None, " ", "existing")],
    )
    def test_empty_location_refuses(self, template, desired_location, existing_location, side):
        decision = self.decide(template, [], [], props(desired_location), props(existing_location))

        assert decision["strategy"] == "refuse"
        assert f"the {side} '{LOCATION_KEY}' is set but empty" in decision["reason"]


class TestGetMvCompatibilityRebuildSql(StrategyMacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(autouse=True)
    def decision(self, context):
        model_config = context["adapter"].get_config_from_model.return_value
        existing_config = context["adapter"].get_relation_config.return_value
        model_config.config = {
            "partition_by": PartitionedByConfig(partition_by=[]),
            "tblproperties": TblPropertiesConfig(tblproperties=props(LOCATION)),
        }
        existing_config.config = {
            "partition_by": PartitionedByConfig(partition_by=["id"]),
            "tblproperties": TblPropertiesConfig(tblproperties=props(LOCATION)),
        }
        context["mv_compatibility_rebuild_strategy"] = Mock()
        context["get_replace_materialized_view_sql"] = Mock(return_value="create or replace mv")
        context["drop_and_create"] = Mock(return_value=["drop mv", "create mv"])
        return context["mv_compatibility_rebuild_strategy"]

    def rebuild(self, template_bundle, existing_relation):
        return self.call(
            template_bundle.template,
            "get_mv_compatibility_rebuild_sql",
            existing_relation,
            template_bundle.relation,
            "select 1",
        )

    def test_passes_desired_and_existing_config(self, template_bundle, decision):
        decision.return_value = None

        assert self.rebuild(template_bundle, Mock()) is None
        decision.assert_called_once_with([], ["id"], props(LOCATION), props(LOCATION))

    def test_replace_uses_create_or_replace(self, template_bundle, context, decision):
        decision.return_value = {"strategy": "replace"}

        assert self.rebuild(template_bundle, Mock()) == "create or replace mv"
        context["drop_and_create"].assert_not_called()

    def test_drop_uses_drop_and_create(self, template_bundle, context, decision):
        existing_relation = Mock()
        decision.return_value = {"strategy": "drop"}

        assert self.rebuild(template_bundle, existing_relation) == ["drop mv", "create mv"]
        context["drop_and_create"].assert_called_once_with(
            existing_relation, template_bundle.relation, "select 1"
        )

    def test_refuse_raises_before_any_drop(self, template_bundle, context, decision):
        decision.return_value = {"strategy": "refuse", "reason": "location would be reused"}

        with pytest.raises(CompilationError, match="location would be reused"):
            self.run_macro_raw(
                template_bundle.template,
                "get_mv_compatibility_rebuild_sql",
                Mock(),
                template_bundle.relation,
                "select 1",
            )
        context["drop_and_create"].assert_not_called()
        context["get_replace_materialized_view_sql"].assert_not_called()
