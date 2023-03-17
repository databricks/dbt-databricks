import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader, PackageLoader
from dbt.adapters.databricks.relation import DatabricksRelation


class TestSparkMacros(unittest.TestCase):
    def setUp(self):
        self.parent_jinja_env = Environment(
            loader=PackageLoader("dbt.include.spark", "macros"),
            extensions=["jinja2.ext.do"],
        )
        self.jinja_env = Environment(
            loader=FileSystemLoader("dbt/include/databricks/macros"),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.var = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "var": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

        self.default_context["var"] = lambda key, default=None, **kwargs: self.var.get(key, default)

    def __get_template(self, template_filename):
        parent = self.parent_jinja_env.get_template(template_filename, globals=self.default_context)
        self.default_context.update(parent.module.__dict__)
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            if hasattr(template.module, f"databricks__{macro_name}"):
                return getattr(template.module, f"databricks__{macro_name}")
            else:
                return self.default_context[f"spark__{macro_name}"]

        self.default_context["adapter"].dispatch = dispatch

        if temporary is not None:
            value = getattr(template.module, name)(temporary, relation, sql)
        else:
            value = getattr(template.module, name)(relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.jinja_env.get_template("adapters.sql")

    def test_macros_create_table_as(self):
        template = self.__get_template("adapters.sql")
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()

        self.assertEqual(sql, "create or replace table my_table using delta as select 1")

    def test_macros_create_table_as_file_format(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "parquet"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table using parquet as select 1")

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table using hudi as select 1")

    def test_macros_create_table_as_options(self):
        template = self.__get_template("adapters.sql")

        self.config["options"] = {"compression": "gzip"}
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            'using delta options (compression "gzip" ) as select 1',
        )

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            'create table my_table using hudi options (compression "gzip" ) as select 1',
        )

    def test_macros_create_table_as_hudi_options(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql,
            'create table my_table using hudi options (primaryKey "id" ) as select 1 as id',
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql,
            'create table my_table using hudi options (primaryKey "id" ) as select 1 as id',
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "uuid"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1 as id"
        )
        self.assertIn("mock.raise_compiler_error()", sql)

    def test_macros_create_table_as_partition(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = "partition_1"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using delta partitioned by (partition_1) as select 1",
        )

    def test_macros_create_table_as_partitions(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = ["partition_1", "partition_2"]
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta partitioned by (partition_1,partition_2) as select 1",
        )

    def test_macros_create_table_as_cluster(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = "cluster_1"
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta clustered by (cluster_1) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_clusters(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta clustered by (cluster_1,cluster_2) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_location(self):
        template = self.__get_template("adapters.sql")

        self.config["location_root"] = "/mnt/root"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta location '/mnt/root/my_table' as select 1",
        )

    def test_macros_create_table_as_comment(self):
        template = self.__get_template("adapters.sql")

        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta comment 'Description Test' as select 1",
        )

    def test_macros_create_table_as_tblproperties(self):
        template = self.__get_template("adapters.sql")

        self.config["tblproperties"] = {"delta.appendOnly": "true"}
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table "
            "using delta tblproperties ('delta.appendOnly' = 'true' ) as select 1",
        )

    def test_macros_create_table_as_all(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.config["tblproperties"] = {"delta.appendOnly": "true"}
        self.default_context["model"].description = "Description Test"

        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
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

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, "my_table", "select 1"
        ).strip()
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
        template = self.__get_template("adapters.sql")

        self.config["tblproperties"] = {"tblproperties_to_view": "true"}
        sql = self.__run_macro(
            template, "databricks__create_view_as", None, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace view my_table "
            "tblproperties ('tblproperties_to_view' = 'true' ) as select 1",
        )


class TestDatabricksMacros(unittest.TestCase):
    def setUp(self):
        self.parent_jinja_env = Environment(
            loader=PackageLoader("dbt.include.spark", "macros"),
            extensions=["jinja2.ext.do"],
        )
        self.jinja_env = Environment(
            loader=FileSystemLoader("dbt/include/databricks/macros"),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.var = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "statement": lambda r, caller: r,
            "var": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        self.default_context["var"] = lambda key, default=None, **kwargs: self.var.get(key, default)

    def __get_template(self, template_filename):
        parent = self.parent_jinja_env.get_template(template_filename, globals=self.default_context)
        self.default_context.update(parent.module.__dict__)
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            if hasattr(template.module, f"databricks__{macro_name}"):
                return getattr(template.module, f"databricks__{macro_name}")
            else:
                return self.default_context[f"spark__{macro_name}"]

        self.default_context["adapter"].dispatch = dispatch

        if temporary is not None:
            value = getattr(template.module, name)(temporary, relation, sql)
        elif sql is not None:
            value = getattr(template.module, name)(relation, sql)
        else:
            value = getattr(template.module, name)(relation)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.jinja_env.get_template("adapters.sql")

    def test_macros_create_table_as(self):
        template = self.__get_template("adapters.sql")
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        relation = DatabricksRelation.from_dict(data)
        sql = self.__run_macro(
            template, "databricks__create_table_as", False, relation, "select 1"
        ).strip()

        self.assertEqual(
            sql,
            (
                "create or replace table "
                "`some_database`.`some_schema`.`some_table` "
                "using delta as select 1"
            ),
        )

    def test_macros_get_optimize_sql(self):
        template = self.__get_template("adapters.sql")
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }
        relation = DatabricksRelation.from_dict(data)

        self.config["zorder"] = "foo"
        sql = self.__run_macro(template, "get_optimize_sql", None, relation, None).strip()

        self.assertEqual(
            sql,
            ("optimize " "`some_database`.`some_schema`.`some_table` " "zorder by (foo)"),
        )
        self.config["zorder"] = ["foo", "bar"]
        sql2 = self.__run_macro(template, "get_optimize_sql", None, relation, None).strip()

        self.assertEqual(
            sql2,
            ("optimize " "`some_database`.`some_schema`.`some_table` " "zorder by (foo, bar)"),
        )

    def test_macros_optimize(self):
        template = self.__get_template("adapters.sql")
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }
        relation = DatabricksRelation.from_dict(data)

        self.config["zorder"] = ["foo", "bar"]
        r = self.__run_macro(template, "optimize", None, relation, None).strip()

        self.assertEqual(
            r,
            "run_optimize_stmt",
        )

        self.var["FOO"] = True
        r = self.__run_macro(template, "optimize", None, relation, None).strip()

        self.assertEqual(
            r,
            "run_optimize_stmt",
        )

        self.var["DATABRICKS_SKIP_OPTIMIZE"] = True
        r = self.__run_macro(template, "optimize", None, relation, None).strip()

        self.assertEqual(
            r,
            "",  # should skip
        )

        self.var["databricks_skip_optimize"] = True
        r = self.__run_macro(template, "optimize", None, relation, None).strip()

        self.assertEqual(
            r,
            "",  # should skip
        )

        del self.var["databricks_skip_optimize"]
