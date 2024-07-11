import pytest

from tests.functional.adapter.dlt import fixtures
from dbt.tests import util


class TestDltNotebook:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": fixtures.source_csv, "expected.csv": fixtures.expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": fixtures.base_schema, "title_count.sql": fixtures.dlt_notebook}

    def test_dlt_notebook(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        util.check_relations_equal(project.adapter, ["expected", "title_count"])
