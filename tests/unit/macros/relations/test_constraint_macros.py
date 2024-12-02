from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.column import DatabricksColumn
from tests.unit.macros.base import MacroTestBase


class TestConstraintMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "constraints.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    @pytest.fixture(scope="class", autouse=True)
    def modify_context(self, default_context) -> None:
        # Mock local_md5
        default_context["local_md5"] = lambda s: f"hash({s})"
        default_context["api"] = Mock(Column=DatabricksColumn)

    def render_constraints(self, template, *args):
        return self.run_macro(template, "databricks_constraints_to_dbt", *args)

    def test_macros_databricks_constraints_to_dbt(self, template):
        constraint = {"name": "name", "condition": "id > 0"}
        r = self.render_constraints(template, [constraint])

        assert r == "[{'name': 'name', 'type': 'check', 'expression': 'id > 0'}]"

    def test_macros_databricks_constraints_missing_name(self, template):
        constraint = {"condition": "id > 0"}
        r = self.render_constraints(template, [constraint])

        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_missing_condition(self, template):
        constraint = {"name": "name", "condition": ""}
        r = self.render_constraints(template, [constraint])

        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_with_type(self, template):
        constraint = {"type": "check", "name": "name", "expression": "id > 0"}
        r = self.render_constraints(template, [constraint])

        assert r == "[{'type': 'check', 'name': 'name', 'expression': 'id > 0'}]"

    def test_macros_databricks_constraints_with_column_missing_expression(self, template):
        column = {"name": "col"}
        constraint = {"name": "name", "condition": "id > 0"}
        r = self.render_constraints(template, [constraint], column)
        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_with_column_and_expression(self, template):
        column = {"name": "col"}
        constraint = {"type": "check", "name": "name", "expression": "id > 0"}
        r = self.render_constraints(template, [constraint], column)

        assert r == "[{'type': 'check', 'name': 'name', 'expression': 'id > 0'}]"

    def test_macros_databricks_constraints_with_column_not_null(self, template):
        column = {"name": "col"}
        constraint = "not_null"
        r = self.render_constraints(template, [constraint], column)

        assert r == "[{'type': 'not_null', 'columns': ['col']}]"

    @pytest.fixture(scope="class")
    def constraint_model(self):
        columns = {
            "id": {"name": "id", "data_type": "int"},
            "name": {"name": "name", "data_type": "string"},
        }
        return {
            "columns": columns,
            "constraints": [{"type": "not_null", "columns": ["id", "name"]}],
        }

    def render_model_constraints(self, template, model):
        return self.run_macro(template, "get_model_constraints", model)

    def test_macros_get_model_constraints(self, template, constraint_model):
        r = self.render_model_constraints(template, constraint_model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def test_macros_get_model_constraints_persist(self, config, template, constraint_model):
        config["persist_constraints"] = True
        r = self.render_model_constraints(template, constraint_model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def test_macros_get_model_constraints_persist_with_meta(
        self, config, template, constraint_model
    ):
        config["persist_constraints"] = True
        constraint_model["meta"] = {"constraints": [{"type": "foo"}]}
        r = self.render_model_constraints(template, constraint_model)

        expected = "[{'type': 'foo'}]"
        assert expected in r

    def test_macros_get_model_constraints_no_persist_with_meta(
        self, config, template, constraint_model
    ):
        config["persist_constraints"] = False
        constraint_model["meta"] = {"constraints": [{"type": "foo"}]}
        r = self.render_model_constraints(template, constraint_model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def render_column_constraints(self, template, column):
        return self.run_macro(template, "get_column_constraints", column)

    def test_macros_get_column_constraints(self, template):
        column = {"name": "id"}
        r = self.render_column_constraints(template, column)

        assert r == "[]"

    def test_macros_get_column_constraints_empty(self, config, template):
        column = {"name": "id"}
        column["constraints"] = []
        config["persist_constraints"] = True
        r = self.render_column_constraints(template, column)

        assert r == "[]"

    def test_macros_get_column_constraints_non_null(self, config, template):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        config["persist_constraints"] = True
        r = self.render_column_constraints(template, column)

        r == "[{'type': 'non_null'}]"

    def test_macros_get_column_constraints_invalid_meta(self, config, template):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        config["persist_constraints"] = True
        column["meta"] = {"constraint": "foo"}
        r = self.render_column_constraints(template, column)

        assert "raise_compiler_error" in r

    def test_macros_get_column_constraints_valid_meta(self, config, template):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        config["persist_constraints"] = True
        column["meta"] = {"constraint": "not_null"}
        r = self.render_column_constraints(template, column)

        assert r == "[{'type': 'not_null', 'columns': ['id']}]"

    def test_macros_get_column_constraints_no_persist(self, config, template):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        config["persist_constraints"] = False
        r = self.render_column_constraints(template, column)

        r == "[{'type': 'non_null'}]"

    def render_constraint_sql(self, template_bundle, constraint, *args):
        return self.run_macro(
            template_bundle.template,
            "get_constraint_sql",
            template_bundle.relation,
            constraint,
            *args,
        )

    @pytest.fixture(scope="class")
    def model(self):
        columns = {
            "id": {"name": "id", "data_type": "int"},
            "name": {"name": "name", "data_type": "string"},
        }
        return {"columns": columns}

    def test_macros_get_constraint_sql_not_null_with_columns(self, template_bundle, model):
        r = self.render_constraint_sql(
            template_bundle, {"type": "not_null", "columns": ["id", "name"]}, model
        )
        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` change column id "
            "set not null ;', 'alter table `some_database`.`some_schema`.`some_table` "
            "change column name set not null ;']"
        )

        assert expected in r

    def test_macros_get_constraint_sql_not_null_with_column(self, template_bundle, model):
        r = self.render_constraint_sql(
            template_bundle, {"type": "not_null"}, model, model["columns"]["id"]
        )

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` change column id "
            "set not null ;']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_not_null_mismatched_columns(self, template_bundle, model):
        r = self.render_constraint_sql(
            template_bundle,
            {"type": "not_null", "columns": ["name"]},
            model,
            model["columns"]["id"],
        )

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` change column name "
            "set not null ;']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_check(self, template_bundle, model):
        constraint = {
            "type": "check",
            "expression": "id != name",
            "name": "myconstraint",
            "columns": ["id", "name"],
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint check (id != name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_check_named_constraint(self, template_bundle, model):
        constraint = {
            "type": "check",
            "expression": "id != name",
            "name": "myconstraint",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint check (id != name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_check_noname_constraint(self, template_bundle, model):
        constraint = {
            "type": "check",
            "expression": "id != name",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` "
            "add constraint hash(some_table;;id != name;) "
            "check (id != name);']"
        )  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_check_missing_expression(self, template_bundle, model):
        constraint = {
            "type": "check",
            "expression": "",
            "name": "myconstraint",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)
        assert "raise_compiler_error" in r

    def test_macros_get_constraint_sql_primary_key(self, template_bundle, model):
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
            "columns": ["name"],
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint primary key(name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_primary_key_with_specified_column(
        self, template_bundle, model
    ):
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
            "columns": ["name"],
        }
        column = {"name": "id"}
        r = self.render_constraint_sql(template_bundle, constraint, model, column)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint primary key(name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_primary_key_with_name(self, template_bundle, model):
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
        }
        column = {"name": "id"}
        r = self.render_constraint_sql(template_bundle, constraint, model, column)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint primary key(id);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_primary_key_noname(self, template_bundle, model):
        constraint = {"type": "primary_key"}
        column = {"name": "id"}

        r = self.render_constraint_sql(template_bundle, constraint, model, column)

        expected = (
            '["alter table `some_database`.`some_schema`.`some_table` add constraint '
            "hash(primary_key;some_table;['id'];) "
            'primary key(id);"]'
        )
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key(self, template_bundle, model):
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name"],
            "to": "parent_table",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add "
            "constraint myconstraint foreign key(name) references "
            "some_schema.parent_table;']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_noname(self, template_bundle, model):
        constraint = {
            "type": "foreign_key",
            "columns": ["name"],
            "to": "parent_table",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            '["alter table `some_database`.`some_schema`.`some_table` add '
            "constraint hash(foreign_key;some_table;['name'];some_schema.parent_table;) "
            'foreign key(name) references some_schema.parent_table;"]'
        )
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_parent_column(self, template_bundle, model):
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name"],
            "to": "parent_table",
            "to_columns": ["parent_name"],
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add "
            "constraint myconstraint foreign key(name) references "
            "some_schema.parent_table(parent_name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_multiple_columns(self, template_bundle, model):
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name", "id"],
            "to": "parent_table",
            "to_columns": ["parent_name", "parent_id"],
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint foreign key(name, id) "
            "references some_schema.parent_table(parent_name, parent_id);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_columns_supplied_separately(
        self, template_bundle, model
    ):
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "to": "parent_table",
            "to_columns": ["parent_name"],
        }
        column = {"name": "id"}
        r = self.render_constraint_sql(template_bundle, constraint, model, column)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint foreign key(id) references "
            "some_schema.parent_table(parent_name);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_custom(self, template_bundle, model):
        constraint = {
            "type": "custom",
            "name": "myconstraint",
            "expression": "PRIMARY KEY(valid_at TIMESERIES)",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` add constraint "
            "myconstraint PRIMARY KEY(valid_at TIMESERIES);']"
        )
        assert expected in r

    def test_macros_get_constraint_sql_custom_noname_constraint(self, template_bundle, model):
        constraint = {
            "type": "custom",
            "expression": "PRIMARY KEY(valid_at TIMESERIES)",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)

        expected = (
            "['alter table `some_database`.`some_schema`.`some_table` "
            "add constraint hash(some_table;PRIMARY KEY(valid_at TIMESERIES);) "
            "PRIMARY KEY(valid_at TIMESERIES);']"
        )  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_custom_missing_expression(self, template_bundle, model):
        constraint = {
            "type": "check",
            "expression": "",
            "name": "myconstraint",
        }
        r = self.render_constraint_sql(template_bundle, constraint, model)
        assert "raise_compiler_error" in r
