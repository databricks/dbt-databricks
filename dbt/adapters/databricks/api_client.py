from requests.exceptions import HTTPError
from typing import Any, Dict

from databricks_cli.sdk.api_client import ApiClient


class Api12Client:
    def __init__(self, host: str, token: str, command_name: str = ""):
        self._api_client = ApiClient(
            host=f"https://{host}",
            token=token,
            api_version="1.2",
            command_name=command_name,
        )
        self.Context = Context(self._api_client)
        self.Command = Command(self._api_client)

    def close(self) -> None:
        self._api_client.close()


class Context:
    def __init__(self, client: ApiClient):
        self.client = client

    def create(self, cluster_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#create-an-execution-context
        try:
            response = self.client.perform_query(
                method="POST",
                path="/contexts/create",
                data=dict(clusterId=cluster_id, language="python"),
            )
            return response["id"]
        except HTTPError as e:
            raise HTTPError(
                f"Error creating an execution context\n {e.response.content!r}", response=e.response
            ) from e

    def destroy(self, cluster_id: str, context_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#delete-an-execution-context
        try:
            response = self.client.perform_query(
                method="POST",
                path="/contexts/destroy",
                data=dict(clusterId=cluster_id, contextId=context_id),
            )
            return response["id"]
        except HTTPError as e:
            raise HTTPError(
                f"Error deleting an execution context\n {e.response.content!r}", response=e.response
            ) from e


class Command:
    def __init__(self, client: ApiClient):
        self.client = client

    def execute(self, cluster_id: str, context_id: str, command: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        try:
            response = self.client.perform_query(
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

    def status(self, cluster_id: str, context_id: str, command_id: str) -> Dict[str, Any]:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#get-information-about-a-command
        try:
            return self.client.perform_query(
                method="GET",
                path="/commands/status",
                data=dict(clusterId=cluster_id, contextId=context_id, commandId=command_id),
            )
        except HTTPError as e:
            return e.response.json()
