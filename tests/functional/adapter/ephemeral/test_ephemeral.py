import os
import re

import pytest

from dbt.tests import util
from dbt.tests.adapter.ephemeral import test_ephemeral
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeral, BaseEphemeralMulti


class TestEphemeralMulti(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        util.run_dbt(["seed"])
        results = util.run_dbt(["run"])
        assert len(results) == 3

        util.check_relations_equal(project.adapter, ["seed", "dependent"])
        util.check_relations_equal(project.adapter, ["seed", "double_dependent"])
        util.check_relations_equal(project.adapter, ["seed", "super_dependent"])


class TestEphemeralNested(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_level_two.sql": test_ephemeral.models_n__ephemeral_level_two_sql,
            "root_view.sql": test_ephemeral.models_n__root_view_sql,
            "ephemeral.sql": test_ephemeral.models_n__ephemeral_sql,
            "source_table.sql": test_ephemeral.models_n__source_table_sql,
        }

    def test_ephemeral_nested(self, project):
        results = util.run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            f"create or replace view `{project.database}`.`test_test_ephemeral`.`root_view` as "
            "with __dbt__cte__ephemeral_level_two as ("
            f"select * from `{project.database}`.`test_test_ephemeral`.`source_table`"
            "),  __dbt__cte__ephemeral as ("
            "select * from __dbt__cte__ephemeral_level_two"
            ")select * from __dbt__cte__ephemeral"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralErrorHandling(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": test_ephemeral.ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": test_ephemeral.ephemeral_errors__base__base_sql,
                "base_copy.sql": test_ephemeral.ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = util.run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message
