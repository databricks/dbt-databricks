from unittest import mock

import pytest


@pytest.fixture(autouse=True)
def _guard_unmocked_requests_get():
    """Force any HTTP call inside probe.py to be explicitly mocked at the
    test level. Catches the "forgot to mock" failure mode with a loud
    AssertionError rather than letting it silently hit the network."""
    with mock.patch(
        "dbt.adapters.databricks.spog.probe.requests.get",
        return_value=mock.Mock(status_code=200, text="ok"),
    ):
        yield
