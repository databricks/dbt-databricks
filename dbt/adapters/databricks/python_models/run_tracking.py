import threading
from typing import Set

from dbt.adapters.databricks.api_client import CommandExecution
from dbt.adapters.databricks.api_client import DatabricksApiClient
from dbt.adapters.databricks.logging import logger


class PythonRunTracker(object):
    _run_ids: Set[str] = set()
    _commands: Set[CommandExecution] = set()
    _lock = threading.Lock()

    @classmethod
    def remove_run_id(cls, run_id: str) -> None:
        cls._lock.acquire()
        try:
            cls._run_ids.discard(run_id)
        finally:
            cls._lock.release()

    @classmethod
    def insert_run_id(cls, run_id: str) -> None:
        cls._lock.acquire()
        try:
            cls._run_ids.add(run_id)
        finally:
            cls._lock.release()

    @classmethod
    def remove_command(cls, command: CommandExecution) -> None:
        cls._lock.acquire()
        try:
            cls._commands.discard(command)
        finally:
            cls._lock.release()

    @classmethod
    def insert_command(cls, command: CommandExecution) -> None:
        cls._lock.acquire()
        try:
            cls._commands.add(command)
        finally:
            cls._lock.release()

    @classmethod
    def cancel_runs(cls, client: DatabricksApiClient) -> None:
        cls._lock.acquire()

        try:
            logger.debug(f"Run_ids to cancel: {cls._run_ids}")

            for run_id in cls._run_ids:
                client.job_runs.cancel(run_id)

            logger.debug(f"Commands to cancel: {cls._commands}")
            for command in cls._commands:
                client.commands.cancel(command)

        finally:
            cls._run_ids.clear()
            cls._commands.clear()
            cls._lock.release()
