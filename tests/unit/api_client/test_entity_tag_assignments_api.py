from unittest.mock import Mock, patch

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.catalog import EntityTagAssignment, EntityTagAssignmentsAPI
from dbt_common.exceptions import DbtRuntimeError
from requests import Request

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

    @pytest.mark.parametrize(
        "entity_name, encoded_name",
        [
            ("main.analytics.orders", "main.analytics.orders"),
            ("main.analytics.orders#2026", "main.analytics.orders%232026"),
            ("main.analytics.orders?2026", "main.analytics.orders%3F2026"),
            ("main.analytics.orders%2026", "main.analytics.orders%252026"),
            ("main.analytics.orders 2026", "main.analytics.orders%202026"),
        ],
    )
    def test_list_table_tags_preserves_entity_name_in_request_path(self, entity_name, encoded_name):
        api_client = Mock()
        api_client._cfg.workspace_id = None
        api_client.do.return_value = {
            "tag_assignments": [{"tag_key": "classification", "tag_value": "internal"}]
        }
        workspace_client = Mock()
        workspace_client.entity_tag_assignments = EntityTagAssignmentsAPI(api_client)

        tags = EntityTagAssignmentsApi(workspace_client).list_table_tags(entity_name)

        method, path = api_client.do.call_args.args
        request = Request(
            method,
            f"https://workspace.example{path}",
            params=api_client.do.call_args.kwargs["query"],
        ).prepare()
        assert request.path_url == (
            f"/api/2.1/unity-catalog/entity-tag-assignments/tables/{encoded_name}/tags?max_results=50"
        )
        assert tags == {"classification": "internal"}

    def test_list_table_tags_returns_none_after_permission_denied(self):
        workspace_client = Mock()
        workspace_client.entity_tag_assignments.list.side_effect = PermissionDenied(
            "Provided access token does not have required scopes: unity-catalog"
        )
        api = EntityTagAssignmentsApi(workspace_client)

        assert api.list_table_tags("main.analytics.orders") is None
        assert api.list_table_tags("main.analytics.customers") is None
        workspace_client.entity_tag_assignments.list.assert_called_once()

    def test_list_table_tags_wraps_sdk_errors(self):
        workspace_client = Mock()
        workspace_client.entity_tag_assignments.list.side_effect = RuntimeError("rate limited")

        with pytest.raises(
            DbtRuntimeError,
            match="Error fetching table tags for main.analytics.orders",
        ):
            EntityTagAssignmentsApi(workspace_client).list_table_tags("main.analytics.orders")
