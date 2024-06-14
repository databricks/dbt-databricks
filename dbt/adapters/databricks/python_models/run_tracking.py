import threading
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set

from dbt.adapters.databricks.logging import logger
from requests import Session


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class CommandExecution(object):
    command_id: str
    context_id: str
    cluster_id: str

    def model_dump(self) -> Dict[str, Any]:
        return {
            "commandId": self.command_id,
            "contextId": self.context_id,
            "clusterId": self.cluster_id,
        }


class PythonRunTracker(object):
    _run_ids: Set[str] = set()
    _commands: Set[CommandExecution] = set()
    _lock = threading.Lock()
    _host: Optional[str] = None

    @classmethod
    def set_host(cls, host: Optional[str]) -> None:
        cls._host = host

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
    def cancel_runs(cls, session: Session) -> None:
        cls._lock.acquire()
        try:
            logger.debug(f"Run_ids to cancel: {cls._run_ids}")
            for run_id in cls._run_ids:
                logger.debug(f"Cancelling run id {run_id}")
                response = session.post(
                    f"https://{cls._host}/api/2.1/jobs/runs/cancel",
                    json={"run_id": run_id},
                )

                if response.status_code != 200:
                    logger.warning(f"Cancel run {run_id} failed.\n {response.content!r}")

            logger.debug(f"Commands to cancel: {cls._commands}")
            for command in cls._commands:
                logger.debug(f"Cancelling command {command}")
                response = session.post(
                    f"https://{cls._host}/api/1.2/commands/cancel",
                    json=command.model_dump(),
                )

                if response.status_code != 200:
                    logger.warning(f"Cancel command {command} failed.\n {response.content!r}")
        finally:
            cls._run_ids.clear()
            cls._commands.clear()
            cls._lock.release()
