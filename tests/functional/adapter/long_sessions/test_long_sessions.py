import pytest
import os
from unittest import mock
from dbt.tests import util
from tests.functional.adapter.long_sessions import fixtures
from dbt.adapters.databricks import connections


class TestLongSessionsBase:
    args_formatter = ""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source.csv": fixtures.source,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "target.sql": fixtures.target,
            "target2.sql": fixtures.target2,
            "target3.sql": fixtures.target3,
        }

    def test_long_sessions(self, project):
        connections.USE_LONG_SESSIONS = True
        _, log = util.run_dbt_and_capture(["--debug", "seed"])
        open_count = log.count("Sending request: OpenSession")
        assert open_count == 4

        _, log = util.run_dbt_and_capture(["--debug", "run"])
        open_count = log.count("Sending request: OpenSession")
        assert open_count == 4


class TestLongSessionsMultipleThreads(TestLongSessionsBase):
    def test_long_sessions(self, project):
        connections.USE_LONG_SESSIONS = True
        _, log = util.run_dbt_and_capture(["--debug", "seed"])
        open_count = log.count("Sending request: OpenSession")
        assert open_count == 4

        _, log = util.run_dbt_and_capture(["--debug", "run", "--threads", "2"])
        open_count = log.count("Sending request: OpenSession")
        assert open_count == 6

        _, log = util.run_dbt_and_capture(["--debug", "run", "--threads", "3"])
        open_count = log.count("Sending request: OpenSession")
        assert open_count == 8
