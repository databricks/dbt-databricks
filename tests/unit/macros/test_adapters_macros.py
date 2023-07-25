from dbt.adapters.databricks.relation import DatabricksRelation

from tests.unit.macros.base import TestMacros


class TestAdaptersMacros(TestMacros):
    def setUp(self):
        super().setUp()
        self.template = self._get_template("adapters.sql")

    def _render_create_table_as(self, relation="my_table", temporary=False, sql="select 1"):
        self.default_context["model"].alias = relation

        return self._run_macro("databricks__create_table_as", temporary, relation, sql)


class TestSparkMacros(TestAdaptersMacros):
    def test_macros_create_table_as(self):
        sql = self._render_create_table_as()

        self.assertEqual(sql, "create or replace table my_table using delta as select 1")

    def test_macros_create_table_as_file_format(self):
        for format in ["parquet", "hudi"]:
            self.config["file_format"] = format
            sql = self._render_create_table_as()
            self.assertEqual(sql, f"create table my_table using {format} as select 1")

    def test_macros_create_table_as_options(self):
        self.config["options"] = {"compression": "gzip"}
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            'using delta options (compression "gzip" ) as select 1',
        )

    def test_macros_create_table_as_hudi_unique_key(self):
        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        sql = self._render_create_table_as(sql="select 1 as id")

        self.assertEqual(
            sql,
            'create table my_table using hudi options (primaryKey "id" ) as select 1 as id',
        )

    def test_macros_create_table_as_hudi_unique_key_primary_key_match(self):
        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        self.config["options"] = {"primaryKey": "id"}
        sql = self._render_create_table_as(sql="select 1 as id")

        self.assertEqual(
            sql,
            'create table my_table using hudi options (primaryKey "id" ) as select 1 as id',
        )

    def test_macros_create_table_as_hudi_unique_key_primary_key_mismatch(self):
        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "uuid"
        self.config["options"] = {"primaryKey": "id"}
        sql = self._render_create_table_as(sql="select 1 as id, 2 as uuid")
        self.assertIn("mock.raise_compiler_error()", sql)

    def test_macros_create_table_as_partition(self):
        self.config["partition_by"] = "partition_1"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table using delta partitioned by (partition_1) as select 1",
        )

    def test_macros_create_table_as_partitions(self):
        self.config["partition_by"] = ["partition_1", "partition_2"]
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta partitioned by (partition_1,partition_2) as select 1",
        )

    def test_macros_create_table_as_cluster(self):
        self.config["clustered_by"] = "cluster_1"
        self.config["buckets"] = "1"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta clustered by (cluster_1) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_clusters(self):
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta clustered by (cluster_1,cluster_2) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_location(self):
        self.config["location_root"] = "/mnt/root"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta location '/mnt/root/my_table' as select 1",
        )

    def test_macros_create_table_as_comment(self):
        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"

        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta comment 'Description Test' as select 1",
        )

    def test_macros_create_table_as_tblproperties(self):
        self.config["tblproperties"] = {"delta.appendOnly": "true"}
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta tblproperties ('delta.appendOnly' = 'true' ) as select 1",
        )

    def test_macros_create_table_as_all_delta(self):
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.config["tblproperties"] = {"delta.appendOnly": "true"}
        self.default_context["model"].description = "Description Test"

        self.config["file_format"] = "delta"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta "
            "partitioned by (partition_1,partition_2) "
            "clustered by (cluster_1,cluster_2) into 1 buckets "
            "location '/mnt/root/my_table' "
            "comment 'Description Test' "
            "tblproperties ('delta.appendOnly' = 'true' ) "
            "as select 1",
        )

    def test_macros_create_table_as_all_hudi(self):
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.config["tblproperties"] = {"delta.appendOnly": "true"}
        self.default_context["model"].description = "Description Test"

        self.config["file_format"] = "hudi"
        sql = self._render_create_table_as()

        self.assertEqual(
            sql,
            "create table my_table "
            "using hudi "
            "partitioned by (partition_1,partition_2) "
            "clustered by (cluster_1,cluster_2) into 1 buckets "
            "location '/mnt/root/my_table' "
            "comment 'Description Test' "
            "tblproperties ('delta.appendOnly' = 'true' ) "
            "as select 1",
        )

    def test_macros_create_view_as_tblproperties(self):
        self.config["tblproperties"] = {"tblproperties_to_view": "true"}
        self.default_context["model"].alias = "my_table"

        sql = self._run_macro("databricks__create_view_as", "my_table", "select 1")

        self.assertEqual(
            sql,
            "create or replace view my_table "
            "tblproperties ('tblproperties_to_view' = 'true' ) as select 1",
        )


class TestDatabricksMacros(TestAdaptersMacros):
    def setUp(self):
        super().setUp()
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        self.relation = DatabricksRelation.from_dict(data)

    def test_macros_create_table_as(self):
        sql = self._render_create_table_as(self.relation)

        self.assertEqual(
            sql,
            (
                "create or replace table "
                "`some_database`.`some_schema`.`some_table` "
                "using delta as select 1"
            ),
        )

    def __render_relation_macro(self, name, *args):
        self.default_context["model"].alias = self.relation

        return self._run_macro(name, self.relation, *args)

    def test_macros_get_optimize_sql(self):
        self.config["zorder"] = "foo"
        sql = self.__render_relation_macro("get_optimize_sql")

        self.assertEqual(
            sql,
            ("optimize " "`some_database`.`some_schema`.`some_table` " "zorder by (foo)"),
        )

    def test_macro_get_optimize_sql_multiple_args(self):
        self.config["zorder"] = ["foo", "bar"]
        sql = self.__render_relation_macro("get_optimize_sql")

        self.assertEqual(
            sql,
            ("optimize " "`some_database`.`some_schema`.`some_table` " "zorder by (foo, bar)"),
        )

    def test_macros_optimize_with_extraneous_info(self):
        self.config["zorder"] = ["foo", "bar"]
        self.var["FOO"] = True
        r = self.__render_relation_macro("optimize")

        self.assertEqual(
            r,
            "run_optimize_stmt",
        )

    def test_macros_optimize_with_skip(self):
        for key_val in ["DATABRICKS_SKIP_OPTIMIZE", "databricks_skip_optimize"]:
            self.var[key_val] = True
            r = self.__render_relation_macro("optimize")

            self.assertEqual(
                r,
                "",  # should skip
            )

            del self.var[key_val]

    def __render_constraints(self, *args):
        self.default_context["model"].alias = self.relation

        return self._run_macro("databricks_constraints_to_dbt", *args)

    def test_macros_databricks_constraints_to_dbt(self):
        constraint = {"name": "name", "condition": "id > 0"}
        r = self.__render_constraints([constraint])

        self.assertEquals(r, "[{'name': 'name', 'type': 'check', 'expression': 'id > 0'}]")

    def test_macros_databricks_constraints_missing_name(self):
        constraint = {"condition": "id > 0"}
        r = self.__render_constraints([constraint])

        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_missing_condition(self):
        constraint = {"name": "name", "condition": ""}
        r = self.__render_constraints([constraint])

        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_with_type(self):
        constraint = {"type": "check", "name": "name", "expression": "id > 0"}
        r = self.__render_constraints([constraint])

        self.assertEquals(r, "[{'type': 'check', 'name': 'name', 'expression': 'id > 0'}]")

    def test_macros_databricks_constraints_with_column_missing_expression(self):
        column = {"name": "col"}
        constraint = {"name": "name", "condition": "id > 0"}
        r = self.__render_constraints([constraint], column)
        assert "raise_compiler_error" in r

    def test_macros_databricks_constraints_with_column_and_expression(self):
        column = {"name": "col"}
        constraint = {"type": "check", "name": "name", "expression": "id > 0"}
        r = self.__render_constraints([constraint], column)

        self.assertEquals(r, "[{'type': 'check', 'name': 'name', 'expression': 'id > 0'}]")

    def test_macros_databricks_constraints_with_column_not_null(self):
        column = {"name": "col"}
        constraint = "not_null"
        r = self.__render_constraints([constraint], column)

        self.assertEquals(r, "[{'type': 'not_null', 'columns': ['col']}]")

    def __constraint_model(self):
        columns = {
            "id": {"name": "id", "data_type": "int"},
            "name": {"name": "name", "data_type": "string"},
        }
        return {
            "columns": columns,
            "constraints": [{"type": "not_null", "columns": ["id", "name"]}],
        }

    def __render_model_constraints(self, model):
        self.default_context["model"].alias = self.relation

        return self._run_macro("get_model_constraints", model)

    def test_macros_get_model_constraints(self):
        model = self.__constraint_model()
        r = self.__render_model_constraints(model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def test_macros_get_model_constraints_persist(self):
        self.config["persist_constraints"] = True
        model = self.__constraint_model()
        r = self.__render_model_constraints(model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def test_macros_get_model_constraints_persist_with_meta(self):
        self.config["persist_constraints"] = True
        model = self.__constraint_model()
        model["meta"] = {"constraints": [{"type": "foo"}]}
        r = self.__render_model_constraints(model)

        expected = "[{'type': 'foo'}]"
        assert expected in r

    def test_macros_get_model_constraints_no_persist_with_meta(self):
        self.config["persist_constraints"] = False
        model = self.__constraint_model()
        model["meta"] = {"constraints": [{"type": "foo"}]}
        r = self.__render_model_constraints(model)

        expected = "[{'type': 'not_null', 'columns': ['id', 'name']}]"
        assert expected in r

    def __render_column_constraints(self, column):
        self.default_context["model"].alias = self.relation

        return self._run_macro("get_column_constraints", column)

    def test_macros_get_column_constraints(self):
        column = {"name": "id"}
        r = self.__render_column_constraints(column)

        self.assertEqual(r, "[]")

    def test_macros_get_column_constraints_empty(self):
        column = {"name": "id"}
        column["constraints"] = []
        self.config["persist_constraints"] = True
        r = self.__render_column_constraints(column)

        self.assertEqual(r, "[]")

    def test_macros_get_column_constraints_non_null(self):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        self.config["persist_constraints"] = True
        r = self.__render_column_constraints(column)

        self.assertEqual(r, "[{'type': 'non_null'}]")

    def test_macros_get_column_constraints_invalid_meta(self):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        self.config["persist_constraints"] = True
        column["meta"] = {"constraint": "foo"}
        r = self.__render_column_constraints(column)

        assert "raise_compiler_error" in r

    def test_macros_get_column_constraints_valid_meta(self):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        self.config["persist_constraints"] = True
        column["meta"] = {"constraint": "not_null"}
        r = self.__render_column_constraints(column)

        self.assertEqual(r, "[{'type': 'not_null', 'columns': ['id']}]")

    def test_macros_get_column_constraints_no_persist(self):
        column = {"name": "id"}
        column["constraints"] = [{"type": "non_null"}]
        self.config["persist_constraints"] = False
        r = self.__render_column_constraints(column)

        self.assertEqual(r, "[{'type': 'non_null'}]")

    def __render_constraint_sql(self, constraint, *args):
        self.default_context["model"].alias = self.relation

        return self._run_macro("get_constraint_sql", self.relation, constraint, *args)

    def __model(self):
        columns = {
            "id": {"name": "id", "data_type": "int"},
            "name": {"name": "name", "data_type": "string"},
        }
        return {"columns": columns}

    def test_macros_get_constraint_sql_not_null_with_columns(self):
        model = self.__model()
        r = self.__render_constraint_sql({"type": "not_null", "columns": ["id", "name"]}, model)
        expected = "['alter table `some_database`.`some_schema`.`some_table` change column id set not null ;', 'alter table `some_database`.`some_schema`.`some_table` change column name set not null ;']"  # noqa: E501

        assert expected in r

    def test_macros_get_constraint_sql_not_null_with_column(self):
        model = self.__model()
        r = self.__render_constraint_sql({"type": "not_null"}, model, model["columns"]["id"])

        expected = "['alter table `some_database`.`some_schema`.`some_table` change column id set not null ;']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_not_null_mismatched_columns(self):
        model = self.__model()
        r = self.__render_constraint_sql(
            {"type": "not_null", "columns": ["name"]}, model, model["columns"]["id"]
        )

        expected = "['alter table `some_database`.`some_schema`.`some_table` change column name set not null ;']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_check(self):
        model = self.__model()
        constraint = {
            "type": "check",
            "expression": "id != name",
            "name": "myconstraint",
            "columns": ["id", "name"],
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint check (id != name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_check_named_constraint(self):
        model = self.__model()
        constraint = {
            "type": "check",
            "expression": "id != name",
            "name": "myconstraint",
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint check (id != name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_check_none_constraint(self):
        model = self.__model()
        constraint = {
            "type": "check",
            "expression": "id != name",
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint None check (id != name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_check_missing_expression(self):
        model = self.__model()
        constraint = {
            "type": "check",
            "expression": "",
            "name": "myconstraint",
        }
        r = self.__render_constraint_sql(constraint, model)
        assert "raise_compiler_error" in r

    def test_macros_get_constraint_sql_primary_key(self):
        model = self.__model()
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
            "columns": ["name"],
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint primary key(name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_primary_key_with_specified_column(self):
        model = self.__model()
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
            "columns": ["name"],
        }
        column = {"name": "id"}
        r = self.__render_constraint_sql(constraint, model, column)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint primary key(name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_primary_key_with_name(self):
        model = self.__model()
        constraint = {
            "type": "primary_key",
            "name": "myconstraint",
        }
        column = {"name": "id"}
        r = self.__render_constraint_sql(constraint, model, column)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint primary key(id);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key(self):
        model = self.__model()
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name"],
            "parent": "parent_table",
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint foreign key(name) references some_schema.parent_table;']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_parent_column(self):
        model = self.__model()
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name"],
            "parent": "parent_table",
            "parent_columns": ["parent_name"],
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint foreign key(name) references some_schema.parent_table(parent_name);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_multiple_columns(self):
        model = self.__model()
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "columns": ["name", "id"],
            "parent": "parent_table",
            "parent_columns": ["parent_name", "parent_id"],
        }
        r = self.__render_constraint_sql(constraint, model)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint foreign key(name, id) references some_schema.parent_table(parent_name, parent_id);']"  # noqa: E501
        assert expected in r

    def test_macros_get_constraint_sql_foreign_key_columns_supplied_separately(self):
        model = self.__model()
        constraint = {
            "type": "foreign_key",
            "name": "myconstraint",
            "parent": "parent_table",
            "parent_columns": ["parent_name"],
        }
        column = {"name": "id"}
        r = self.__render_constraint_sql(constraint, model, column)

        expected = "['alter table `some_database`.`some_schema`.`some_table` add constraint myconstraint foreign key(id) references some_schema.parent_table(parent_name);']"  # noqa: E501
        assert expected in r
