import pytest
from dbt_common.exceptions.macros import MacroReturn

from tests.unit.macros.base import MacroTestBase


class TestUseMaterializationV2(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self):
        return "use_materialization_v2.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self):
        return ["macros/materializations", "macros"]

    @pytest.fixture
    def default_context(self):
        context = MacroTestBase.default_context.__wrapped__(self)

        def macro_return(value):
            raise MacroReturn(value)

        context["return"] = macro_return
        return context

    def resolve(self, template_bundle, context, project_flag):
        context["adapter"].get_behavior_flag_no_warn.return_value = project_flag
        with pytest.raises(MacroReturn) as result:
            self.run_macro_raw(template_bundle.template, "use_materialization_v2")
        return result.value.value

    @pytest.mark.parametrize("project_flag", [False, True])
    def test_unset_model_setting_follows_project_flag(self, template_bundle, context, project_flag):
        assert self.resolve(template_bundle, context, project_flag) is project_flag
        context["adapter"].get_behavior_flag_no_warn.assert_called_once_with(
            "use_materialization_v2"
        )

    @pytest.mark.parametrize("project_flag", [False, True])
    def test_null_model_setting_follows_project_flag(
        self, template_bundle, config, context, project_flag
    ):
        config["use_materialization_v2"] = None
        assert self.resolve(template_bundle, context, project_flag) is project_flag

    @pytest.mark.parametrize(
        "project_flag,model_setting,expected",
        [
            pytest.param(False, True, True, id="opt-in"),
            pytest.param(True, False, False, id="opt-out"),
            pytest.param(False, "true", True, id="opt-in-string"),
            pytest.param(True, "False", False, id="opt-out-string"),
        ],
    )
    def test_model_setting_overrides_project_flag(
        self, template_bundle, config, context, project_flag, model_setting, expected
    ):
        config["use_materialization_v2"] = model_setting
        assert self.resolve(template_bundle, context, project_flag) is expected
