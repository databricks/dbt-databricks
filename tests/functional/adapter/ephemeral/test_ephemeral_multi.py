from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from dbt.tests import util


class TestDatabricksEphemeralMulti(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        util.run_dbt(["seed"])
        results = util.run_dbt(["run"])
        assert len(results) == 3

        util.check_relations_equal(project.adapter, ["seed", "dependent"])
        util.check_relations_equal(project.adapter, ["seed", "double_dependent"])
        util.check_relations_equal(project.adapter, ["seed", "super_dependent"])
