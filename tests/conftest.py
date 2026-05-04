import logging
import os
import re
import threading
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from tests.profiles import get_databricks_cluster_target

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    # Use DBT_DATABRICKS_PROFILE env var if set, otherwise default to databricks_uc_sql_endpoint
    default_profile = os.environ.get("DBT_DATABRICKS_PROFILE", "databricks_uc_sql_endpoint")
    parser.addoption("--profile", action="store", default=default_profile, type=str)


_HTTP_TRACE_ENABLED = os.environ.get("DBT_DATABRICKS_HTTP_TRACE", "0") == "1"
_HTTP_STATUS_RE = re.compile(r"\b(?:HTTP/\d\.\d\s+|status[_=:\s]+|<\s+)(\d{3})\b")
_status_counter: dict[str, int] = defaultdict(int)
_counter_lock = threading.Lock()


class _LongMessageDrop(logging.Filter):
    def filter(self, record):
        try:
            return len(record.getMessage()) <= 5000
        except Exception:
            return True


class _StatusCodeCounter(logging.Filter):
    def filter(self, record):
        try:
            msg = record.getMessage()
        except Exception:
            return True
        for m in _HTTP_STATUS_RE.finditer(msg):
            with _counter_lock:
                _status_counter[m.group(1)] += 1
        return True


def _worker_id(config):
    workerinput = getattr(config, "workerinput", None)
    if workerinput:
        return workerinput.get("workerid", "gw?")
    return "main"


def _setup_http_trace(config):
    if not _HTTP_TRACE_ENABLED:
        return
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    wid = _worker_id(config)
    handler = RotatingFileHandler(
        log_dir / f"http_trace_{wid}.log",
        maxBytes=100_000_000,
        backupCount=3,
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    handler.addFilter(_LongMessageDrop())
    handler.addFilter(_StatusCodeCounter())
    for name in ("databricks.sdk", "databricks.sql", "urllib3"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.propagate = False
    config._http_trace_worker_id = wid


# Using @pytest.mark.skip_profile('databricks_cluster') uses the 'skip_by_adapter_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
    _setup_http_trace(config)


def pytest_sessionfinish(session, exitstatus):
    if not _HTTP_TRACE_ENABLED:
        return
    wid = getattr(session.config, "_http_trace_worker_id", "main")
    summary = Path("logs") / f"http_status_counts_{wid}.txt"
    with _counter_lock:
        with summary.open("w") as f:
            for code, count in sorted(_status_counter.items()):
                f.write(f"{code}\t{count}\n")


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    return get_databricks_cluster_target(profile_type)


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")


# The profile dictionary, used to write out profiles.yml. It will pull in updates
# from two separate sources, the 'profile_target' and 'profiles_config_update'.
# The second one is useful when using alternative targets, etc.
@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    profile = {
        "test": {
            "outputs": {
                "default": {},
            },
            "target": "default",
        },
    }
    target = dbt_profile_target
    target["schema"] = unique_schema

    # For testing model-level compute override
    target["compute"] = {
        "alternate_uc_cluster": {
            "http_path": get_databricks_cluster_target("databricks_uc_cluster")["http_path"]
        }
    }
    profile["test"]["outputs"]["default"] = target

    alternate_warehouse = target.copy()
    alternate_warehouse["compute"] = {
        "alternate_warehouse": {"http_path": dbt_profile_target["http_path"]},
        "alternate_warehouse2": {"http_path": dbt_profile_target["http_path"]},
        "alternate_warehouse3": {"http_path": dbt_profile_target["http_path"]},
    }
    profile["test"]["outputs"]["alternate_warehouse"] = alternate_warehouse

    idle_sessions = alternate_warehouse.copy()
    idle_sessions["connect_max_idle"] = 1
    profile["test"]["outputs"]["idle_sessions"] = idle_sessions

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile
