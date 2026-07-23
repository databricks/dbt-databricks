from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.catalog import EntityTagAssignment
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.api_client import DatabricksApiClient, EntityTagAssignmentsApi


class TestEntityTagAssignmentsApi:
    @patch("dbt.adapters.databricks.api_client.DatabricksCredentialManager.create_from")
    def test_databricks_api_client_exposes_entity_tag_assignments(self, create_from):
        workspace_client = Mock()
        create_from.return_value.api_client = workspace_client

        api_client = DatabricksApiClient(Mock(), timeout=60)

        assert api_client.entity_tag_assignments.workspace_client is workspace_client

    def test_list_table_tags(self):
        workspace_client = Mock()
        workspace_client.entity_tag_assignments.list.return_value = [
            EntityTagAssignment(
                entity_name="main.analytics.orders",
                entity_type="tables",
                tag_key="classification",
                tag_value="internal",
            ),
            EntityTagAssignment(
                entity_name="main.analytics.orders",
                entity_type="tables",
                tag_key="key_only",
            ),
        ]

        tags = EntityTagAssignmentsApi(workspace_client).list_table_tags("main.analytics.orders")

        assert tags == {"classification": "internal", "key_only": ""}
        workspace_client.entity_tag_assignments.list.assert_called_once_with(
            entity_type="tables",
            entity_name="main.analytics.orders",
            max_results=50,
        )

    def test_list_table_tags_wraps_sdk_errors(self):
        workspace_client = Mock()
        workspace_client.entity_tag_assignments.list.side_effect = RuntimeError("rate limited")

        with pytest.raises(
            DbtRuntimeError,
            match="Error fetching table tags for main.analytics.orders",
        ):
            EntityTagAssignmentsApi(workspace_client).list_table_tags("main.analytics.orders")
