import pytest

from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraint_model.sql": fixtures.constraint_model,
            "schema.yml": fixtures.schema_without_constraints,
        }

    def test_changing_constraints(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.schema_with_constraints, "models", "schema.yml")
        util.run_dbt(["run"])
        # TODO: verifications
