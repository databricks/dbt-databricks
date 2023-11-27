import pytest
import os
from unittest import mock
from dbt.tests import util
from tests.functional.adapter.long_sessions import fixtures

with mock.patch.dict(os.environ, {"DBT_DATABRICKS_LONG_SESSIONS": "true"}):
    import dbt.adapters.databricks.connections  # noqa


class TestLongSessionsBase:
    args_formatter = ""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": fixtures.source,
        }

    @pytest.fixture(scope="class")
    def models(self):
        m = {}
        for i in range(5):
            m[f"target{i}.sql"] = fixtures.target

        return m

    def test_long_sessions(self, project):
        # connections.USE_LONG_SESSIONS = True
        _, log = util.run_dbt_and_capture(["--debug", "seed"])
        open_count = log.count("Sending request: OpenSession") / 2
        assert open_count == 2

        _, log = util.run_dbt_and_capture(["--debug", "run"])
        open_count = log.count("Sending request: OpenSession") / 2
        assert open_count == 2


class TestLongSessionsMultipleThreads(TestLongSessionsBase):
    def test_long_sessions(self, project):
        util.run_dbt_and_capture(["seed"])

        for n_threads in [1, 2, 3]:
            _, log = util.run_dbt_and_capture(["--debug", "run", "--threads", f"{n_threads}"])
            open_count = log.count("Sending request: OpenSession") / 2
            assert open_count == (n_threads + 1)
