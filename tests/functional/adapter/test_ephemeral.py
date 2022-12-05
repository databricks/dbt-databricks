from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeralMulti
from dbt.tests.util import run_dbt, check_relations_equal


class TestEphemeralMultiDatabricks(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
