import os
from typing import Dict, cast
import uuid

from requests import HTTPError

import dbt.exceptions
from dbt.adapters.spark.python_submissions import BaseDatabricksHelper

from dbt.adapters.databricks.__version__ import version
from dbt.adapters.databricks.api_client import Api12Client
from dbt.adapters.databricks.connections import DatabricksCredentials, DBT_DATABRICKS_INVOCATION_ENV


class CommandApiPythonJobHelper(BaseDatabricksHelper):
    credentials: DatabricksCredentials  # type: ignore[assignment]

    def __init__(self, parsed_model: Dict, credentials: DatabricksCredentials) -> None:
        super().__init__(
            parsed_model=parsed_model, credentials=credentials  # type: ignore[arg-type]
        )

        command_name = f"dbt-databricks_{version}"
        invocation_env = os.environ.get(DBT_DATABRICKS_INVOCATION_ENV)
        if invocation_env is not None and len(invocation_env) > 0:
            command_name = f"{command_name}-{invocation_env}"
        command_name += "-" + str(uuid.uuid1())

        self.api_client = Api12Client(
            host=cast(str, credentials.host),
            token=cast(str, credentials.token),
            command_name=command_name,
        )

    def check_credentials(self) -> None:
        if not self.cluster_id:
            raise ValueError("Databricks cluster is required for commands submission method.")

    def submit(self, compiled_code: str) -> None:
        cluster_id = self.cluster_id

        try:
            # Create an execution context
            context_id = self.api_client.Context.create(cluster_id)

            try:
                # Run a command
                command_id = self.api_client.Command.execute(
                    cluster_id=cluster_id,
                    context_id=context_id,
                    command=compiled_code,
                )

                # poll until job finish
                response = self.polling(
                    status_func=self.api_client.Command.status,
                    status_func_kwargs=dict(
                        cluster_id=cluster_id, context_id=context_id, command_id=command_id
                    ),
                    get_state_func=lambda response: response["status"],
                    terminal_states=("Cancelled", "Error", "Finished"),
                    expected_end_state="Finished",
                    get_state_msg_func=lambda response: response.json()["results"]["data"],
                )
                if response["results"]["resultType"] == "error":
                    raise dbt.exceptions.RuntimeException(
                        f"Python model failed with traceback as:\n"
                        f"{response['results']['cause']}"
                    )
            finally:
                # Delete the execution context
                self.api_client.Context.destroy(cluster_id=cluster_id, context_id=context_id)

        except HTTPError as e:
            raise dbt.exceptions.RuntimeException(str(e)) from e
