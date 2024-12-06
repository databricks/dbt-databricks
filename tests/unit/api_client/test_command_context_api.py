from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import ContextStatus
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import CommandContextApi
from tests.unit.api_client.api_test_base import ApiTestBase


class TestCommandContextApi(ApiTestBase):
    @pytest.fixture
    def api(self, client):
        return CommandContextApi(client)

    @pytest.fixture
    def command_client(self, client):
        return client.command_execution

    def test_create__error(self, api, command_client):
        command_client.create_and_wait.return_value = Mock(status=ContextStatus.ERROR)
        with pytest.raises(DbtRuntimeError, match="Error creating an execution context"):
            api.create("cluster_id")

    def test_create__success(self, api, command_client):
        command_client.create_and_wait.return_value = Mock(
            status=ContextStatus.PENDING, id="context_id"
        )
        assert api.create("cluster_id") == "context_id"
