from dbt.tests import util
from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency, seeds__update_csv


# Copied from dbt-core
class TestConcurrency(BaseConcurrency):
    def test_concurrency(self, project):
        util.run_dbt(["seed", "--select", "seed"])
        results = util.run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        util.check_relations_equal(project.adapter, ["seed", "view_model"])
        util.check_relations_equal(project.adapter, ["seed", "dep"])
        util.check_relations_equal(project.adapter, ["seed", "table_a"])
        util.check_relations_equal(project.adapter, ["seed", "table_b"])
        util.check_table_does_not_exist(project.adapter, "invalid")
        util.check_table_does_not_exist(project.adapter, "skip")

        util.rm_file(project.project_root, "seeds", "seed.csv")
        util.write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = util.run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7
        util.check_relations_equal(project.adapter, ["seed", "view_model"])
        util.check_relations_equal(project.adapter, ["seed", "dep"])
        util.check_relations_equal(project.adapter, ["seed", "table_a"])
        util.check_relations_equal(project.adapter, ["seed", "table_b"])
        util.check_table_does_not_exist(project.adapter, "invalid")
        util.check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1" in output
