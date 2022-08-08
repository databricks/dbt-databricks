from requests.exceptions import HTTPError
from typing import Any, Dict

from databricks_cli.sdk.api_client import ApiClient


class Api12Client:
    def __init__(self, host: str, token: str):
        self._api_client = ApiClient(
            user="token",
            password=token,
            host=f"https://{host}",
            api_version="1.2",
            command_name="dbt-databricks",
        )

    def create_context(self, cluster_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#create-an-execution-context
        try:
            response = self._api_client.perform_query(
                method="POST",
                path="/contexts/create",
                data=dict(clusterId=cluster_id, language="python"),
            )
            return response["id"]
        except HTTPError as e:
            raise HTTPError(
                f"Error creating an execution context\n {e.response.content!r}", response=e.response
            ) from e

    def destroy_context(self, cluster_id: str, context_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#delete-an-execution-context
        try:
            response = self._api_client.perform_query(
                method="POST",
                path="/contexts/destroy",
                data=dict(clusterId=cluster_id, contextId=context_id),
            )
            return response["id"]
        except HTTPError as e:
            raise HTTPError(
                f"Error deleting an execution context\n {e.response.content!r}", response=e.response
            ) from e

    def execute_command(self, cluster_id: str, context_id: str, command: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        try:
            response = self._api_client.perform_query(
                method="POST",
                path="/commands/execute",
                data=dict(
                    clusterId=cluster_id,
                    contextId=context_id,
                    language="python",
                    command=command,
                ),
            )
            return response["id"]
        except HTTPError as e:
            raise HTTPError(
                f"Error creating a command\n {e.response.content!r}", response=e.response
            ) from e

    def command_status(self, cluster_id: str, context_id: str, command_id: str) -> Dict[str, Any]:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#get-information-about-a-command
        try:
            return self._api_client.perform_query(
                method="GET",
                path="/commands/status",
                data=dict(clusterId=cluster_id, contextId=context_id, commandId=command_id),
            )
        except HTTPError as e:
            return e.response.json()
