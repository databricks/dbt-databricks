import pytest
from dbt.tests import util

from tests.functional.adapter.microbatch.fixtures import (
    microbatch_model_reordered_sql,
    microbatch_model_sql,
    microbatch_seeds_csv,
    schema_yml,
)


class TestMicrobatchInsertByName:
    """
    Verifies that the microbatch strategy uses INSERT BY NAME, which preserves
    data integrity when column order changes between batches (DBR 12.2+).
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_model.sql": microbatch_model_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "microbatch_seeds.csv": microbatch_seeds_csv,
        }

    def test_microbatch_insert_by_name(self, project):
        # Initial run — creates the table with original column order
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        # Verify initial data using direct SQL comparison and fully qualified name
        actual_initial = project.run_sql(
            f"select id, amount from {project.database}.{project.test_schema}.microbatch_model order by id",
            fetch="all",
        )
        assert len(actual_initial) == 3
        assert actual_initial[0] == (1, 100)
        assert actual_initial[1] == (2, 200)
        assert actual_initial[2] == (3, 300)

        # Simulate column order change by swapping to reordered model
        # Add new data to ensure an incremental batch is processed
        new_seeds_csv = microbatch_seeds_csv.strip() + "\n4,2023-01-03,400\n"
        util.write_file(new_seeds_csv, str(project.project_root) + "/seeds/microbatch_seeds.csv")

        util.write_file(
            microbatch_model_reordered_sql,
            str(project.project_root) + "/models/microbatch_model.sql",
        )

        # Second run — should correctly insert by name, not position
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        # Verify row count and data integrity are correct
        actual_final = project.run_sql(
            f"select id, amount from {project.database}.{project.test_schema}.microbatch_model order by id",
            fetch="all",
        )

        assert len(actual_final) == 4
        assert actual_final[0] == (1, 100)
        assert actual_final[1] == (2, 200)
        assert actual_final[2] == (3, 300)

        # The critical assertion: amount must be 400, not 4 (which id=4 would produce positionally)
        # Without BY NAME, the data from the reordered select would map into the wrong columns.
        assert actual_final[3] == (4, 400)
