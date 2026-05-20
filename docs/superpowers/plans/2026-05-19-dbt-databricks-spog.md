# dbt-databricks SPOG support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SPOG (Single Point of Gateway) support to dbt-databricks. Users connecting to account-level vanity URLs (e.g. `peco.azuredatabricks.net`) with `?o=<workspace-id>` in `http_path` get correct routing for both data-plane (SQL) and control-plane (REST) calls. Feature is opt-in via dep ceiling bumps; floors stay.

**Architecture:** Auto-derive `workspace_id` from `http_path?o=…`. Pass `http_path` unchanged to `dbsql.connect(...)` (the connector ≥ 4.2.6 handles SPOG transparently). Pass extracted `workspace_id` to `Config(workspace_id=...)` for the SDK. At `connection.open()`, probe `/.well-known/databricks-config` (cached, with retry) and apply a decision matrix that hard-errors on misconfiguration. Capability-detect dep versions so users on pre-SPOG deps continue working unchanged.

**Tech Stack:** Python 3.10+, `databricks-sql-connector` (≥4.2.6 for SPOG), `databricks-sdk` (≥0.104.0 for SPOG), `packaging` (version comparison), `requests` (probe), `hatch` (build/test runner), `pytest`.

**Spec reference:** `docs/superpowers/specs/2026-05-19-dbt-databricks-spog-design.md`

---

## Pre-implementation verification (required gate; no code changes)

Run the four probes from spec §10 against `peco.azuredatabricks.net?o=6436897454825492` with the existing `DBT_DATABRICKS_*` env vars. Capture output; attach to the PR description. If any probe fails its expected outcome, **stop** — triage with the SPOG team before continuing.

- [ ] **Step 1: Verify discovery endpoint**

```bash
curl -sf https://peco.azuredatabricks.net/.well-known/databricks-config | python -m json.tool
```

Expected: HTTP 200, JSON body containing `"host_type": "unified"` and an `account_id` field.

- [ ] **Step 2: Verify SDK control-plane works on SPOG**

In a fresh venv with `databricks-sdk>=0.104.0` installed:

```bash
DATABRICKS_HOST=https://peco.azuredatabricks.net \
DATABRICKS_TOKEN=$DBT_DATABRICKS_TOKEN \
DATABRICKS_WORKSPACE_ID=6436897454825492 \
python -c "from databricks.sdk import WorkspaceClient; print([f.path for f in WorkspaceClient().workspace.list('/')][:3])"
```

Expected: Prints up to 3 paths from the workspace root. No 4xx, no routing errors.

- [ ] **Step 3: Verify connector data-plane works on SPOG**

In a fresh venv with `databricks-sql-connector>=4.2.6` installed:

```bash
python -c "
import databricks.sql as dbsql
import os
conn = dbsql.connect(
    server_hostname='peco.azuredatabricks.net',
    http_path=os.environ['DBT_DATABRICKS_HTTP_PATH'] + '?o=6436897454825492',
    access_token=os.environ['DBT_DATABRICKS_TOKEN'],
)
with conn.cursor() as c:
    c.execute('SELECT 1')
    print(c.fetchone())
"
```

Expected: Prints `(1,)`. No connection error.

- [ ] **Step 4: Verify negative case (missing `?o=`)**

Same as Step 3 but **without** `?o=...` in `http_path`:

```bash
python -c "
import databricks.sql as dbsql
import os
conn = dbsql.connect(
    server_hostname='peco.azuredatabricks.net',
    http_path=os.environ['DBT_DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DBT_DATABRICKS_TOKEN'],
)
with conn.cursor() as c:
    c.execute('SELECT 1')
    print(c.fetchone())
" 2>&1 | tee /tmp/spog-negative.log
```

Expected: Fails. Inspect the error/response for the header `x-databricks-popp-routing-reason: workspace-id`. Save the exact error surface for use in Task 7 (decision matrix error mapping).

- [ ] **Step 5: Record findings**

Attach Steps 1-4 outputs to the implementation PR description. If Step 4 *succeeds* (no error), the proxy is more permissive than the design doc implies — pause and re-evaluate whether always-on probe is justified.

---

## Phase 1 — Always-on changes (no behavior change for existing users)

These changes ship in PR 1. Tiny, safe diff. Existing tests continue to pass; no new dep version is required to install.

### Task 1: Bump dep ceilings and declare `packaging`

**Files:**
- Modify: `pyproject.toml:25-27`

- [ ] **Step 1: Edit `pyproject.toml`**

Locate the `dependencies = [...]` block. Change:

```diff
 dependencies = [
     "click>=8.2.0, <9.0.0",
-    "databricks-sdk>=0.68.0, <0.78.0",
-    "databricks-sql-connector[pyarrow]>=4.1.1, <4.1.6",
+    "databricks-sdk>=0.68.0, <0.105.0",
+    "databricks-sql-connector[pyarrow]>=4.1.1, <4.3.0",
+    "packaging>=23.0",
     ...
```

- [ ] **Step 2: Refresh hatch env to pick up new pins**

Run:

```bash
hatch env prune && hatch run python -c "import packaging.version; print(packaging.version.__version__)"
```

Expected: prints a `packaging` version ≥ 23.0.

- [ ] **Step 3: Verify install of SPOG-capable versions works**

Run:

```bash
hatch run python -c "
import databricks.sql, databricks.sdk
print('connector:', databricks.sql.__version__)
print('sdk:', databricks.sdk.version.__version__)
"
```

Expected: Both print versions. Connector may still be ≤ 4.1.x in the existing env — that's fine (ceiling allows ≤ 4.2.x; user must upgrade for SPOG). Test only that the install resolves.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): bump connector/sdk ceilings and declare packaging

Open ceilings to allow installation of SPOG-capable versions:
- databricks-sql-connector[pyarrow]: <4.1.6 -> <4.3.0
- databricks-sdk: <0.78.0 -> <0.105.0
Floors unchanged; SPOG is opt-in. Add explicit packaging dep
(used for robust version comparison in upcoming SPOG capability
detection)."
```

### Task 2: Fix cluster-id extraction regex

**Files:**
- Modify: `dbt/adapters/databricks/credentials.py:21`
- Test: `tests/unit/test_credentials_cluster_id_regex.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_credentials_cluster_id_regex.py`:

```python
from dbt.adapters.databricks.credentials import DatabricksCredentials


class TestExtractClusterId:
    def test_legacy_cluster_path(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_leading_slash(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_spog_query_param(self):
        # ?o=<workspace-id> must NOT be captured into the cluster id
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=6436897454825492"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_cluster_path_with_multiple_query_params(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=64&ts=1"
            )
            == "0605-142813-rf81cyrh"
        )

    def test_warehouse_path_returns_none(self):
        assert (
            DatabricksCredentials.extract_cluster_id(
                "/sql/1.0/warehouses/abc123?o=6436897454825492"
            )
            is None
        )
```

- [ ] **Step 2: Run test to verify failure**

```bash
hatch run unit tests/unit/test_credentials_cluster_id_regex.py -v
```

Expected: `test_cluster_path_with_spog_query_param` and `test_cluster_path_with_multiple_query_params` FAIL — current regex captures the `?o=...` suffix.

- [ ] **Step 3: Fix the regex**

In `dbt/adapters/databricks/credentials.py:21`:

```diff
-EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/(.*)")
+EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/([^?&]+)")
```

- [ ] **Step 4: Run test to verify pass**

```bash
hatch run unit tests/unit/test_credentials_cluster_id_regex.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Run full unit suite for regression check**

```bash
hatch run unit -v
```

Expected: No new failures introduced.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_credentials_cluster_id_regex.py dbt/adapters/databricks/credentials.py
git commit -m "fix(spog): stop cluster-id extraction at query string

The greedy '(.*)' capture in EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX
swallowed any '?o=<workspace-id>' suffix into the cluster id. Change
to '([^?&]+)' so the capture stops at the first '?' or '&'. Net zero
risk on legacy paths (which have no query string)."
```

### Task 3: Create `spog/extract.py` for `?o=` parsing

**Files:**
- Create: `dbt/adapters/databricks/spog/__init__.py`
- Create: `dbt/adapters/databricks/spog/extract.py`
- Test: `tests/unit/spog/__init__.py`
- Test: `tests/unit/spog/test_extract.py`

- [ ] **Step 1: Create empty package markers**

```bash
mkdir -p dbt/adapters/databricks/spog tests/unit/spog
touch dbt/adapters/databricks/spog/__init__.py tests/unit/spog/__init__.py
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/spog/test_extract.py`:

```python
from dbt.adapters.databricks.spog.extract import extract_workspace_id


class TestExtractWorkspaceId:
    def test_warehouse_path_with_o_param(self):
        assert (
            extract_workspace_id("/sql/1.0/warehouses/abc123?o=6436897454825492")
            == "6436897454825492"
        )

    def test_cluster_path_with_o_param(self):
        assert (
            extract_workspace_id(
                "/sql/protocolv1/o/2548836972759138/0605-142813-rf81cyrh?o=6436897454825492"
            )
            == "6436897454825492"
        )

    def test_no_query_string(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123") is None

    def test_query_string_no_o_param(self):
        assert extract_workspace_id("/sql/1.0/warehouses/abc123?other=value") is None

    def test_multiple_query_params(self):
        assert (
            extract_workspace_id("/sql/1.0/warehouses/abc123?o=12345&ts=1") == "12345"
        )

    def test_o_param_not_first(self):
        assert (
            extract_workspace_id("/sql/1.0/warehouses/abc123?ts=1&o=12345") == "12345"
        )

    def test_empty_string(self):
        assert extract_workspace_id("") is None

    def test_none_input(self):
        assert extract_workspace_id(None) is None

    def test_duplicate_o_params_returns_first(self):
        # parse_qs returns ['a', 'b']; we take the first to be deterministic
        assert extract_workspace_id("/path?o=a&o=b") == "a"
```

- [ ] **Step 3: Run test to verify failure**

```bash
hatch run unit tests/unit/spog/test_extract.py -v
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'dbt.adapters.databricks.spog.extract'`.

- [ ] **Step 4: Implement `extract.py`**

Create `dbt/adapters/databricks/spog/extract.py`:

```python
"""Parse the ?o=<workspace-id> SPOG routing query param out of an http_path."""

from typing import Optional
from urllib.parse import parse_qs


def extract_workspace_id(http_path: Optional[str]) -> Optional[str]:
    """Return the ?o=<workspace-id> value from a Databricks http_path, or None.

    SPOG (Single Point of Gateway) workspaces are disambiguated by a query
    parameter on the http_path. This is a pure parser; no network I/O, no
    classification of whether the host is actually SPOG.

    Args:
        http_path: The http_path string from a profile or compute config.
            May be None or empty.

    Returns:
        The workspace id as a string, or None when http_path has no ?o= param.
        Duplicate ?o= parameters return the first value (deterministic).
    """
    if not http_path or "?" not in http_path:
        return None
    query = http_path.split("?", 1)[1]
    return parse_qs(query).get("o", [None])[0]
```

- [ ] **Step 5: Run test to verify pass**

```bash
hatch run unit tests/unit/spog/test_extract.py -v
```

Expected: All 9 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add dbt/adapters/databricks/spog/__init__.py dbt/adapters/databricks/spog/extract.py tests/unit/spog/__init__.py tests/unit/spog/test_extract.py
git commit -m "feat(spog): add ?o= workspace-id extraction helper

Pure parser that pulls the SPOG routing query param out of an
http_path. Used by both the data-plane wiring (verbatim pass-through
to dbsql.connect) and the SDK-config wiring (Config(workspace_id=...))
in upcoming commits."
```

### Task 4: Create `spog/capabilities.py` for dep-version detection

**Files:**
- Create: `dbt/adapters/databricks/spog/capabilities.py`
- Test: `tests/unit/spog/test_capabilities.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/spog/test_capabilities.py`:

```python
import inspect
from unittest import mock

import pytest

from dbt.adapters.databricks.spog import capabilities


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear @cache decorators between tests so each test sees a fresh state."""
    capabilities.connector_supports_spog.cache_clear()
    capabilities.sdk_supports_workspace_id.cache_clear()
    yield
    capabilities.connector_supports_spog.cache_clear()
    capabilities.sdk_supports_workspace_id.cache_clear()


class TestConnectorSupportsSpog:
    def test_returns_true_for_supported_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_returns_true_for_higher_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.3.0",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_returns_false_for_lower_version(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.1.5",
        ):
            assert capabilities.connector_supports_spog() is False

    def test_handles_alpha_release(self):
        # 4.2.6a1 < 4.2.6 per PEP 440 ordering
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6a1",
        ):
            assert capabilities.connector_supports_spog() is False

    def test_handles_post_release(self):
        # 4.2.6.post1 > 4.2.6 per PEP 440 ordering
        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            return_value="4.2.6.post1",
        ):
            assert capabilities.connector_supports_spog() is True

    def test_missing_package_returns_false(self):
        from importlib.metadata import PackageNotFoundError

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities._pkg_version",
            side_effect=PackageNotFoundError("databricks-sql-connector"),
        ):
            assert capabilities.connector_supports_spog() is False


class TestSdkSupportsWorkspaceId:
    def test_returns_true_when_config_accepts_workspace_id(self):
        # Build a fake Config-like class with a workspace_id parameter
        class FakeConfig:
            def __init__(self, host=None, token=None, workspace_id=None):
                pass

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities.Config", FakeConfig
        ):
            assert capabilities.sdk_supports_workspace_id() is True

    def test_returns_false_when_config_lacks_workspace_id(self):
        class FakeConfig:
            def __init__(self, host=None, token=None):
                pass

        with mock.patch(
            "dbt.adapters.databricks.spog.capabilities.Config", FakeConfig
        ):
            assert capabilities.sdk_supports_workspace_id() is False
```

- [ ] **Step 2: Run test to verify failure**

```bash
hatch run unit tests/unit/spog/test_capabilities.py -v
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'dbt.adapters.databricks.spog.capabilities'`.

- [ ] **Step 3: Implement `capabilities.py`**

Create `dbt/adapters/databricks/spog/capabilities.py`:

```python
"""Detect whether the installed connector and SDK support SPOG.

Floors on databricks-sql-connector and databricks-sdk are intentionally kept
low so users on pre-SPOG versions continue working unchanged. These predicates
let the rest of the adapter decide whether the SPOG code path is reachable
in the current environment.
"""

import inspect
from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from databricks.sdk.core import Config
from packaging.version import Version

CONNECTOR_SPOG_MIN = Version("4.2.6")


@cache
def connector_supports_spog() -> bool:
    """True iff the installed databricks-sql-connector is >= 4.2.6.

    Uses packaging.version.Version for PEP-440-correct ordering across
    alpha / post / dev release suffixes. Cached for the lifetime of the
    process.
    """
    try:
        return Version(_pkg_version("databricks-sql-connector")) >= CONNECTOR_SPOG_MIN
    except PackageNotFoundError:
        return False


@cache
def sdk_supports_workspace_id() -> bool:
    """True iff the installed databricks-sdk Config accepts a workspace_id kwarg.

    Feature-detected via inspect.signature rather than version comparison so
    forks/wrappers that vendor the SDK still report correctly. The workspace_id
    attribute was introduced in databricks-sdk v0.103.0.
    """
    return "workspace_id" in inspect.signature(Config).parameters
```

- [ ] **Step 4: Run test to verify pass**

```bash
hatch run unit tests/unit/spog/test_capabilities.py -v
```

Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/adapters/databricks/spog/capabilities.py tests/unit/spog/test_capabilities.py
git commit -m "feat(spog): add capability detection for connector/sdk SPOG support

Two cached predicates:
- connector_supports_spog(): version-detect databricks-sql-connector >= 4.2.6
  using packaging.version.Version for PEP-440 correctness.
- sdk_supports_workspace_id(): feature-detect Config.workspace_id via
  inspect.signature so forks/wrappers report correctly."
```

---

## Phase 2 — SPOG behavior (capability-gated)

These changes ship in PR 2. Adds the discovery probe, decision matrix, `Config(workspace_id=…)` plumbing, and `dbt debug` extensions. Pre-SPOG users are unaffected because they never trigger the SPOG branches.

### Task 5: Create `spog/probe.py` — happy-path probe

**Files:**
- Create: `dbt/adapters/databricks/spog/probe.py`
- Test: `tests/unit/spog/test_probe.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/spog/test_probe.py`:

```python
from unittest import mock

import pytest
import requests

from dbt.adapters.databricks.spog import probe
from dbt.adapters.databricks.spog.probe import HostMetadata


@pytest.fixture(autouse=True)
def clear_probe_cache():
    probe.probe_host.cache_clear()
    yield
    probe.probe_host.cache_clear()


def _mock_response(json_body, status_code=200):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status = mock.Mock()
    return resp


class TestProbeHostHappyPath:
    def test_unified_host(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response(
                {"host_type": "unified", "account_id": "acct-123"}
            ),
        ) as m:
            result = probe.probe_host("peco.azuredatabricks.net")
        assert result == HostMetadata(host_type="unified", account_id="acct-123")
        m.assert_called_once_with(
            "https://peco.azuredatabricks.net/.well-known/databricks-config",
            timeout=5,
        )

    def test_legacy_host_serves_endpoint_but_not_unified(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "workspace"}),
        ):
            result = probe.probe_host("legacy.azuredatabricks.net")
        assert result.host_type == "workspace"

    def test_result_cached_per_host(self):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=_mock_response({"host_type": "unified"}),
        ) as m:
            probe.probe_host("peco.azuredatabricks.net")
            probe.probe_host("peco.azuredatabricks.net")
        assert m.call_count == 1
```

- [ ] **Step 2: Run test to verify failure**

```bash
hatch run unit tests/unit/spog/test_probe.py -v
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'dbt.adapters.databricks.spog.probe'`.

- [ ] **Step 3: Implement `probe.py` (happy path only — retry comes in Task 6)**

Create `dbt/adapters/databricks/spog/probe.py`:

```python
"""Discovery probe for /.well-known/databricks-config.

Lets the adapter classify a host as SPOG (host_type='unified') or legacy.
Probe is one-shot per host per process; result is cached.

This file ships with the happy-path implementation; retry/backoff is added
in a follow-up commit before any decision matrix consumes it.
"""

from dataclasses import dataclass
from functools import cache
from typing import Optional

import requests

from dbt.adapters.databricks.logging import logger


@dataclass(frozen=True)
class HostMetadata:
    """Subset of /.well-known/databricks-config the adapter consumes."""

    host_type: Optional[str]
    account_id: Optional[str] = None


@cache
def probe_host(host: str) -> HostMetadata:
    """Probe https://{host}/.well-known/databricks-config. Cached per host."""
    url = f"https://{host}/.well-known/databricks-config"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        body = resp.json()
        return HostMetadata(
            host_type=body.get("host_type"),
            account_id=body.get("account_id"),
        )
    except (requests.RequestException, ValueError):
        logger.debug(f"SPOG discovery probe to {url!r} failed")
        return HostMetadata(host_type=None)
```

- [ ] **Step 4: Run test to verify pass**

```bash
hatch run unit tests/unit/spog/test_probe.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/adapters/databricks/spog/probe.py tests/unit/spog/test_probe.py
git commit -m "feat(spog): add discovery probe for /.well-known/databricks-config

One-shot probe per host (cached) that returns HostMetadata with host_type.
Happy-path only; retry/backoff in the next commit. Used by the decision
matrix to classify SPOG (unified) vs legacy (workspace) hosts."
```

### Task 6: Add exponential-backoff retry to `probe_host`

**Files:**
- Modify: `dbt/adapters/databricks/spog/probe.py`
- Modify: `tests/unit/spog/test_probe.py`

- [ ] **Step 1: Write failing tests for retry behavior**

Append to `tests/unit/spog/test_probe.py`:

```python
class TestProbeHostRetry:
    def test_retries_three_times_on_failure(self, caplog):
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            side_effect=requests.ConnectionError("network down"),
        ) as m, mock.patch(
            "dbt.adapters.databricks.spog.probe.time.sleep"
        ) as sleep_mock:
            result = probe.probe_host("flaky.example.com")
        assert m.call_count == 3
        assert sleep_mock.call_count == 2  # sleep between attempts 1->2 and 2->3
        assert result == HostMetadata(host_type=None)
        # WARN must mention the host and the error
        assert any(
            "flaky.example.com" in r.message and "WARNING" in r.levelname.upper()
            or "WARN" in r.levelname.upper()
            for r in caplog.records
        ) or any("flaky.example.com" in str(r) for r in caplog.records)

    def test_recovers_on_second_attempt(self):
        responses = [
            requests.ConnectionError("transient"),
            _mock_response({"host_type": "unified", "account_id": "acct-x"}),
        ]
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            side_effect=responses,
        ) as m, mock.patch(
            "dbt.adapters.databricks.spog.probe.time.sleep"
        ):
            result = probe.probe_host("recovers.example.com")
        assert m.call_count == 2
        assert result.host_type == "unified"

    def test_json_decode_error_treated_as_failure(self):
        bad_resp = mock.Mock(spec=requests.Response)
        bad_resp.status_code = 200
        bad_resp.raise_for_status = mock.Mock()
        bad_resp.json.side_effect = ValueError("not json")
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            return_value=bad_resp,
        ), mock.patch("dbt.adapters.databricks.spog.probe.time.sleep"):
            result = probe.probe_host("badjson.example.com")
        assert result == HostMetadata(host_type=None)
```

- [ ] **Step 2: Run tests to verify failure**

```bash
hatch run unit tests/unit/spog/test_probe.py::TestProbeHostRetry -v
```

Expected: `test_retries_three_times_on_failure` FAILS (currently only 1 attempt). `test_recovers_on_second_attempt` FAILS likewise. `test_json_decode_error_treated_as_failure` passes (already covered) — that's fine.

- [ ] **Step 3: Implement retry**

Replace `probe_host` in `dbt/adapters/databricks/spog/probe.py`:

```python
import random
import time

# ...

@cache
def probe_host(host: str) -> HostMetadata:
    """Probe https://{host}/.well-known/databricks-config. Cached per host.

    Retries up to 3 attempts with exponential backoff (~0.5s, 1s with jitter).
    On exhaustion, logs a WARN and returns HostMetadata(host_type=None) so the
    caller falls back to the legacy code path. Failure is never fatal.
    """
    url = f"https://{host}/.well-known/databricks-config"
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            body = resp.json()
            return HostMetadata(
                host_type=body.get("host_type"),
                account_id=body.get("account_id"),
            )
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < 2:
                time.sleep((0.5 * (2**attempt)) + random.random() * 0.25)

    logger.warning(
        f"SPOG discovery probe to {url!r} failed after 3 attempts "
        f"(last error: {last_exc}). Proceeding as a non-SPOG host. "
        f"If {host} is a SPOG (unified) workspace, routing errors may follow; "
        f"verify network reachability to /.well-known/databricks-config."
    )
    return HostMetadata(host_type=None)
```

- [ ] **Step 4: Run tests to verify pass**

```bash
hatch run unit tests/unit/spog/test_probe.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/adapters/databricks/spog/probe.py tests/unit/spog/test_probe.py
git commit -m "feat(spog): add exponential-backoff retry to discovery probe

Three attempts with ~0.5s/1s backoff + jitter. On exhaustion, logs a
WARN and returns HostMetadata(host_type=None) so callers fall back to
the legacy code path. Probe failure is never fatal."
```

### Task 7: Create `spog/decision.py` — decision matrix enforcement

**Files:**
- Create: `dbt/adapters/databricks/spog/decision.py`
- Test: `tests/unit/spog/test_decision.py`

- [ ] **Step 1: Write the failing tests covering all matrix rows**

Create `tests/unit/spog/test_decision.py`:

```python
from unittest import mock

import pytest
from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.spog import decision
from dbt.adapters.databricks.spog.probe import HostMetadata


def _patch(
    *,
    host_type,
    connector_ok=True,
    sdk_ok=True,
):
    """Patch capability + probe modules to fixed responses for one test."""
    patches = [
        mock.patch(
            "dbt.adapters.databricks.spog.decision.probe_host",
            return_value=HostMetadata(host_type=host_type),
        ),
        mock.patch(
            "dbt.adapters.databricks.spog.decision.connector_supports_spog",
            return_value=connector_ok,
        ),
        mock.patch(
            "dbt.adapters.databricks.spog.decision.sdk_supports_workspace_id",
            return_value=sdk_ok,
        ),
    ]
    return patches


def _apply(patches):
    cms = [p.__enter__() for p in patches]

    def teardown():
        for p in reversed(patches):
            p.__exit__(None, None, None)

    return teardown


class TestCheckSpogPreconditions:
    def test_spog_happy_path(self):
        teardown = _apply(_patch(host_type="unified"))
        try:
            ws_id = decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )
            assert ws_id == "64"
        finally:
            teardown()

    def test_spog_missing_o_raises(self):
        teardown = _apply(_patch(host_type="unified"))
        try:
            with pytest.raises(DbtConfigError, match=r"http_path.*?o="):
                decision.check_spog_preconditions(
                    host="peco.azuredatabricks.net",
                    http_paths=["/sql/1.0/warehouses/abc"],
                )
        finally:
            teardown()

    def test_spog_sdk_too_old_raises(self):
        teardown = _apply(_patch(host_type="unified", sdk_ok=False))
        try:
            with pytest.raises(DbtConfigError, match=r"databricks-sdk.*?>=\s*0\.104"):
                decision.check_spog_preconditions(
                    host="peco.azuredatabricks.net",
                    http_paths=["/sql/1.0/warehouses/abc?o=64"],
                )
        finally:
            teardown()

    def test_spog_connector_too_old_raises(self):
        teardown = _apply(_patch(host_type="unified", connector_ok=False))
        try:
            with pytest.raises(
                DbtConfigError, match=r"databricks-sql-connector.*?>=\s*4\.2\.6"
            ):
                decision.check_spog_preconditions(
                    host="peco.azuredatabricks.net",
                    http_paths=["/sql/1.0/warehouses/abc?o=64"],
                )
        finally:
            teardown()

    def test_non_spog_with_o_raises(self):
        teardown = _apply(_patch(host_type="workspace"))
        try:
            with pytest.raises(DbtConfigError, match=r"not a SPOG"):
                decision.check_spog_preconditions(
                    host="legacy.azuredatabricks.net",
                    http_paths=["/sql/1.0/warehouses/abc?o=64"],
                )
        finally:
            teardown()

    def test_non_spog_legacy_returns_none(self):
        teardown = _apply(_patch(host_type="workspace"))
        try:
            assert (
                decision.check_spog_preconditions(
                    host="legacy.azuredatabricks.net",
                    http_paths=["/sql/1.0/warehouses/abc"],
                )
                is None
            )
        finally:
            teardown()

    def test_probe_failure_permissive_with_o(self, caplog):
        teardown = _apply(_patch(host_type=None))  # probe failed
        try:
            ws_id = decision.check_spog_preconditions(
                host="flaky.example.com",
                http_paths=["/sql/1.0/warehouses/abc?o=64"],
            )
            # Permissive: returns the extracted id, lets request proceed
            assert ws_id == "64"
        finally:
            teardown()

    def test_probe_failure_permissive_without_o(self):
        teardown = _apply(_patch(host_type=None))
        try:
            assert (
                decision.check_spog_preconditions(
                    host="flaky.example.com", http_paths=["/sql/1.0/warehouses/abc"]
                )
                is None
            )
        finally:
            teardown()

    def test_multi_compute_consistent_returns_id(self):
        teardown = _apply(_patch(host_type="unified"))
        try:
            ws_id = decision.check_spog_preconditions(
                host="peco.azuredatabricks.net",
                http_paths=[
                    "/sql/1.0/warehouses/a?o=64",
                    "/sql/1.0/warehouses/b?o=64",
                ],
            )
            assert ws_id == "64"
        finally:
            teardown()

    def test_multi_compute_conflicting_raises(self):
        teardown = _apply(_patch(host_type="unified"))
        try:
            with pytest.raises(DbtConfigError, match=r"conflicting"):
                decision.check_spog_preconditions(
                    host="peco.azuredatabricks.net",
                    http_paths=[
                        "/sql/1.0/warehouses/a?o=64",
                        "/sql/1.0/warehouses/b?o=99",
                    ],
                )
        finally:
            teardown()
```

- [ ] **Step 2: Run tests to verify failure**

```bash
hatch run unit tests/unit/spog/test_decision.py -v
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'dbt.adapters.databricks.spog.decision'`.

- [ ] **Step 3: Implement `decision.py`**

Create `dbt/adapters/databricks/spog/decision.py`:

```python
"""Decision matrix for SPOG preconditions, called from connection.open()."""

from typing import Iterable, Optional

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.spog.capabilities import (
    connector_supports_spog,
    sdk_supports_workspace_id,
)
from dbt.adapters.databricks.spog.extract import extract_workspace_id
from dbt.adapters.databricks.spog.probe import probe_host


def check_spog_preconditions(
    *,
    host: str,
    http_paths: Iterable[str],
) -> Optional[str]:
    """Apply the spec §8 decision matrix. Return the extracted workspace_id
    when the SPOG path is active; return None when the legacy path is active.
    Raise DbtConfigError on misconfiguration.

    The caller is `DatabricksConnectionManager.open()`. http_paths is the
    iterable of all http_path values in play for this connection (the
    default http_path plus any per-compute paths). All paths must agree on
    the workspace_id (or all lack `?o=`).
    """
    extracted = {extract_workspace_id(p) for p in http_paths}
    extracted.discard(None)

    if len(extracted) > 1:
        raise DbtConfigError(
            f"Found conflicting ?o=<workspace-id> values across http_paths "
            f"for host {host!r}: {sorted(extracted)}. Every compute used by "
            f"this connection must agree on the workspace id."
        )

    workspace_id: Optional[str] = next(iter(extracted), None)
    metadata = probe_host(host)

    if metadata.host_type == "unified":
        if workspace_id is None:
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace, but `http_path` "
                f"does not include `?o=<workspace-id>`. Update your profiles.yml "
                f"to: `http_path: /sql/1.0/warehouses/<id>?o=<workspace-id>`. "
                f"The workspace id is visible in the Databricks UI when copying "
                f"the JDBC/ODBC connection details."
            )
        if not connector_supports_spog():
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace. The installed "
                f"`databricks-sql-connector` does not support SPOG (requires "
                f">= 4.2.6). Upgrade with: "
                f"`pip install 'databricks-sql-connector>=4.2.6'`."
            )
        if not sdk_supports_workspace_id():
            raise DbtConfigError(
                f"Host {host!r} is a SPOG (unified) workspace. The installed "
                f"`databricks-sdk` does not support `workspace_id` on Config "
                f"(requires >= 0.104.0). Upgrade with: "
                f"`pip install 'databricks-sdk>=0.104.0'`."
            )
        return workspace_id

    if metadata.host_type is not None and workspace_id is not None:
        raise DbtConfigError(
            f"`http_path` contains `?o={workspace_id}` but host {host!r} is not "
            f"a SPOG workspace (host_type={metadata.host_type!r}). Either "
            f"change `host` to the SPOG hostname for this workspace, or remove "
            f"`?o=` from `http_path`."
        )

    # metadata.host_type is None (probe failed) OR not unified with no ?o=
    # In the probe-failed case with ?o= present, be permissive: return the
    # extracted id so the caller can still plumb it through. The WARN was
    # already logged inside probe_host on failure.
    return workspace_id
```

- [ ] **Step 4: Run tests to verify pass**

```bash
hatch run unit tests/unit/spog/test_decision.py -v
```

Expected: All 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/adapters/databricks/spog/decision.py tests/unit/spog/test_decision.py
git commit -m "feat(spog): add decision matrix for connection.open()

check_spog_preconditions implements spec §8: probes the host, validates
http_path/?o= against host_type, and raises pointed DbtConfigError on
each misconfiguration row. Returns the workspace_id on the happy path
so the caller can plumb it into Config(...) and dbsql.connect(...)."
```

### Task 8: Plumb `workspace_id` into `DatabricksCredentialManager`

**Files:**
- Modify: `dbt/adapters/databricks/credentials.py:269-407` (`DatabricksCredentialManager` class)
- Test: `tests/unit/test_auth.py` (existing — add new tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_auth.py`:

```python
from unittest import mock

import pytest

from dbt.adapters.databricks.credentials import (
    DatabricksCredentialManager,
    DatabricksCredentials,
)


class TestCredentialManagerWorkspaceId:
    def _creds(self, http_path, token="dapi-fake"):
        return DatabricksCredentials(
            host="peco.azuredatabricks.net",
            http_path=http_path,
            token=token,
            database="main",
            schema="default",
        )

    def test_workspace_id_extracted_when_present(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=6436897454825492")
        mgr = DatabricksCredentialManager.create_from(creds)
        assert mgr.workspace_id == "6436897454825492"

    def test_workspace_id_none_when_absent(self):
        creds = self._creds("/sql/1.0/warehouses/abc")
        mgr = DatabricksCredentialManager.create_from(creds)
        assert mgr.workspace_id is None

    def test_pat_config_receives_workspace_id_when_supported(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=64")
        with mock.patch(
            "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
            return_value=True,
        ), mock.patch(
            "dbt.adapters.databricks.credentials.Config"
        ) as cfg:
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        cfg.assert_called_with(
            host="peco.azuredatabricks.net", token="dapi-fake", workspace_id="64"
        )

    def test_pat_config_no_workspace_id_when_unsupported(self):
        creds = self._creds("/sql/1.0/warehouses/abc?o=64")
        with mock.patch(
            "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
            return_value=False,
        ), mock.patch(
            "dbt.adapters.databricks.credentials.Config"
        ) as cfg:
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        cfg.assert_called_with(host="peco.azuredatabricks.net", token="dapi-fake")

    def test_pat_config_no_workspace_id_when_no_o_param(self):
        creds = self._creds("/sql/1.0/warehouses/abc")  # no ?o=
        with mock.patch(
            "dbt.adapters.databricks.credentials.sdk_supports_workspace_id",
            return_value=True,
        ), mock.patch(
            "dbt.adapters.databricks.credentials.Config"
        ) as cfg:
            mgr = DatabricksCredentialManager.create_from(creds)
            mgr.authenticate_with_pat()
        cfg.assert_called_with(host="peco.azuredatabricks.net", token="dapi-fake")
```

- [ ] **Step 2: Run tests to verify failure**

```bash
hatch run unit tests/unit/test_auth.py::TestCredentialManagerWorkspaceId -v
```

Expected: All FAIL — `workspace_id` doesn't yet exist on `DatabricksCredentialManager`, and `authenticate_with_pat` doesn't yet pass it.

- [ ] **Step 3: Update `DatabricksCredentialManager`**

In `dbt/adapters/databricks/credentials.py`, add the import near the existing ones (top of file):

```python
from dbt.adapters.databricks.spog.capabilities import sdk_supports_workspace_id
from dbt.adapters.databricks.spog.extract import extract_workspace_id
```

Add `workspace_id` field to the dataclass (after the existing fields near line 280):

```python
@dataclass
class DatabricksCredentialManager(DataClassDictMixin):
    host: str
    client_id: str
    client_secret: str
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    oauth_redirect_url: str = REDIRECT_URL
    oauth_scopes: list[str] = field(default_factory=lambda: SCOPES)
    token: Optional[str] = None
    auth_type: Optional[str] = None
    workspace_id: Optional[str] = None   # NEW
```

Update `create_from` (around line 281) to set it:

```python
    @classmethod
    def create_from(cls, credentials: DatabricksCredentials) -> "DatabricksCredentialManager":
        return DatabricksCredentialManager(
            host=credentials.host or "",
            token=credentials.token,
            client_id=credentials.client_id or CLIENT_ID,
            client_secret=credentials.client_secret or "",
            azure_client_id=credentials.azure_client_id,
            azure_client_secret=credentials.azure_client_secret,
            oauth_redirect_url=credentials.oauth_redirect_url or REDIRECT_URL,
            oauth_scopes=credentials.oauth_scopes or SCOPES,
            auth_type=credentials.auth_type,
            workspace_id=extract_workspace_id(credentials.http_path),   # NEW
        )
```

Add a small helper just above the `authenticate_with_*` methods (e.g. line 295):

```python
    def _config_kwargs(self, **base: Any) -> dict[str, Any]:
        """Conditionally add workspace_id to Config kwargs when the SDK supports it."""
        if self.workspace_id and sdk_supports_workspace_id():
            base["workspace_id"] = self.workspace_id
        return base
```

Update each `authenticate_with_*` (5 methods total) to use it. PAT example:

```python
    def authenticate_with_pat(self) -> Config:
        return Config(**self._config_kwargs(host=self.host, token=self.token))
```

Apply the same pattern to:

- `authenticate_with_oauth_m2m` — add `workspace_id` via `_config_kwargs(host=..., client_id=..., client_secret=..., auth_type="oauth-m2m")`
- `authenticate_with_external_browser` — same pattern
- `legacy_authenticate_with_azure_client_secret` — same pattern
- `authenticate_with_azure_client_secret` — same pattern

- [ ] **Step 4: Run tests to verify pass**

```bash
hatch run unit tests/unit/test_auth.py -v
```

Expected: All tests PASS (including pre-existing ones — no regressions).

- [ ] **Step 5: Run full unit suite for regression check**

```bash
hatch run unit -v
```

Expected: Existing test count + new tests, all green.

- [ ] **Step 6: Commit**

```bash
git add dbt/adapters/databricks/credentials.py tests/unit/test_auth.py
git commit -m "feat(spog): plumb workspace_id into Config(...) for all auth modes

Extract ?o= from credentials.http_path in create_from; populate
DatabricksCredentialManager.workspace_id. Every authenticate_with_*
method now passes workspace_id to Config when:
  1. extraction returned a value, AND
  2. sdk_supports_workspace_id() is True (capability gate).

Pre-SPOG SDKs and non-SPOG http_paths are unaffected."
```

### Task 9: Wire decision matrix into `DatabricksConnectionManager.open()`

**Files:**
- Modify: `dbt/adapters/databricks/connections.py:470-526` (`open` classmethod)
- Test: `tests/unit/test_connection_manager.py` (existing — add new tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_connection_manager.py`:

```python
from unittest import mock

import pytest
from dbt_common.exceptions import DbtConfigError

from dbt.adapters.databricks.connections import DatabricksConnectionManager


class TestOpenSpogIntegration:
    def test_raises_on_spog_host_without_o(self):
        """connection.open() must call check_spog_preconditions and propagate
        the DbtConfigError. We exercise the wiring, not the matrix logic."""
        with mock.patch(
            "dbt.adapters.databricks.connections.check_spog_preconditions",
            side_effect=DbtConfigError("SPOG missing ?o="),
        ):
            fake_conn = mock.Mock()
            fake_conn.state = "init"
            fake_conn.credentials = mock.Mock(
                host="peco.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/abc",
                compute=None,
                connect_timeout=None,
                connect_retries=1,
                retry_all=False,
                cluster_id=None,
            )
            fake_conn.http_path = "/sql/1.0/warehouses/abc"
            with pytest.raises(DbtConfigError, match="SPOG missing"):
                DatabricksConnectionManager.open(fake_conn)

    def test_passes_all_http_paths_to_check(self):
        """When per-compute paths exist, all of them must be passed."""
        captured = {}

        def fake_check(*, host, http_paths):
            captured["host"] = host
            captured["http_paths"] = list(http_paths)
            return None

        with mock.patch(
            "dbt.adapters.databricks.connections.check_spog_preconditions",
            side_effect=fake_check,
        ), mock.patch(
            "dbt.adapters.databricks.connections.DatabricksHandle.from_connection_args"
        ):
            fake_conn = mock.Mock()
            fake_conn.state = "init"
            fake_conn.credentials = mock.Mock(
                host="peco.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/default?o=64",
                compute={
                    "extra": {"http_path": "/sql/1.0/warehouses/alt?o=64"}
                },
                connect_timeout=None,
                connect_retries=1,
                retry_all=False,
                cluster_id=None,
            )
            fake_conn.http_path = "/sql/1.0/warehouses/default?o=64"
            fake_conn.credentials.authenticate = mock.Mock(
                return_value=mock.Mock()
            )
            DatabricksConnectionManager.open(fake_conn)

        assert captured["host"] == "peco.azuredatabricks.net"
        assert "/sql/1.0/warehouses/default?o=64" in captured["http_paths"]
        assert "/sql/1.0/warehouses/alt?o=64" in captured["http_paths"]
```

- [ ] **Step 2: Run tests to verify failure**

```bash
hatch run unit tests/unit/test_connection_manager.py::TestOpenSpogIntegration -v
```

Expected: FAIL with `ImportError` or `AttributeError` — `check_spog_preconditions` is not yet imported into `connections.py`.

- [ ] **Step 3: Wire `check_spog_preconditions` into `open()`**

In `dbt/adapters/databricks/connections.py`, add the import near the top:

```python
from dbt.adapters.databricks.spog.decision import check_spog_preconditions
```

In `DatabricksConnectionManager.open` (around line 470), add a precondition check before constructing `conn_args`. The new block goes immediately after `cls.credentials_manager = creds.authenticate()`:

```python
    @classmethod
    def open(cls, connection: Connection) -> Connection:
        databricks_connection = cast(DatabricksDBTConnection, connection)

        if connection.state == ConnectionState.OPEN:
            return connection

        creds: DatabricksCredentials = connection.credentials
        timeout = creds.connect_timeout

        cls.credentials_manager = creds.authenticate()

        # SPOG decision matrix: collect every http_path in play (default +
        # per-compute) and validate them against the host's discovery probe.
        # Raises DbtConfigError on misconfiguration; returns None on legacy.
        http_paths = [databricks_connection.http_path]
        if creds.compute:
            for compute_cfg in creds.compute.values():
                p = compute_cfg.get("http_path") if compute_cfg else None
                if p:
                    http_paths.append(p)
        check_spog_preconditions(host=creds.host or "", http_paths=http_paths)

        # ... existing code (merged_query_tags, conn_args, connect, retry) ...
```

(Keep the rest of `open` unchanged.)

- [ ] **Step 4: Run tests to verify pass**

```bash
hatch run unit tests/unit/test_connection_manager.py::TestOpenSpogIntegration -v
```

Expected: PASS.

- [ ] **Step 5: Run full unit suite for regression check**

```bash
hatch run unit -v
```

Expected: All green.

- [ ] **Step 6: Commit**

```bash
git add dbt/adapters/databricks/connections.py tests/unit/test_connection_manager.py
git commit -m "feat(spog): wire decision matrix into DatabricksConnectionManager.open

Collects every http_path in play for the connection (default + per-compute)
and invokes check_spog_preconditions. On the happy path the call is a
no-op (probe cached after the first connection); on misconfiguration it
raises DbtConfigError with the pointed message from spec §8."
```

### Task 10: Extend `dbt debug` output with SPOG status block

**Files:**
- Modify: `dbt/adapters/databricks/impl.py` (find `debug_query` or related diagnostic surface)
- Test: `tests/unit/test_adapter.py`

- [ ] **Step 1: Locate the existing debug-output surface**

Run:

```bash
grep -n "debug_query\|debug_print\|print_debug" dbt/adapters/databricks/impl.py dbt/adapters/databricks/connections.py 2>&1 | head
```

Identify the function that emits the `dbt debug` connection block. (As of this writing, it's typically `debug_query` on the adapter or a `connection_test`-style hook. Adjust the next steps to match whatever the codebase exposes.)

- [ ] **Step 2: Write the failing test**

Append to `tests/unit/test_adapter.py`:

```python
from unittest import mock

from dbt.adapters.databricks.spog.probe import HostMetadata


class TestDebugSurfacesSpogStatus:
    def test_spog_host_diagnostics_block(self, caplog):
        # Arrange: SPOG host, capable deps
        from dbt.adapters.databricks.impl import DatabricksAdapter

        with mock.patch(
            "dbt.adapters.databricks.spog.probe.probe_host",
            return_value=HostMetadata(host_type="unified", account_id="acct"),
        ), mock.patch(
            "dbt.adapters.databricks.spog.capabilities.connector_supports_spog",
            return_value=True,
        ), mock.patch(
            "dbt.adapters.databricks.spog.capabilities.sdk_supports_workspace_id",
            return_value=True,
        ):
            # Call whichever method emits the SPOG block. We assert via log
            # records rather than printed output to remain framework-agnostic.
            DatabricksAdapter.debug_emit_spog_block(
                host="peco.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/abc?o=64",
            )

        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "SPOG host" in joined
        assert "workspace_id" in joined
        assert "64" in joined
        assert "supported" in joined.lower()
```

- [ ] **Step 3: Run test to verify failure**

```bash
hatch run unit tests/unit/test_adapter.py::TestDebugSurfacesSpogStatus -v
```

Expected: FAIL with `AttributeError: ... has no attribute 'debug_emit_spog_block'`.

- [ ] **Step 4: Implement `debug_emit_spog_block` on `DatabricksAdapter`**

Add to `dbt/adapters/databricks/impl.py`:

```python
from dbt.adapters.databricks.spog.capabilities import (
    connector_supports_spog,
    sdk_supports_workspace_id,
)
from dbt.adapters.databricks.spog.extract import extract_workspace_id
from dbt.adapters.databricks.spog.probe import probe_host
from importlib.metadata import version as _pkg_version


class DatabricksAdapter(...):   # existing class
    # ... existing methods ...

    @staticmethod
    def debug_emit_spog_block(host: str, http_path: str) -> None:
        """Emit a diagnostic block describing SPOG status; called from dbt debug."""
        metadata = probe_host(host)
        is_spog = metadata.host_type == "unified"
        workspace_id = extract_workspace_id(http_path)

        connector_v = _pkg_version("databricks-sql-connector")
        sdk_v = _pkg_version("databricks-sdk")
        connector_ok = connector_supports_spog()
        sdk_ok = sdk_supports_workspace_id()

        logger.info(f"  SPOG host (host_type={metadata.host_type!r}): "
                    f"{'yes' if is_spog else 'no'}")
        logger.info(f"  workspace_id (from ?o= in http_path): {workspace_id!r}")
        logger.info(
            f"  databricks-sql-connector version: {connector_v} "
            f"({'supported' if connector_ok else 'NOT supported (>= 4.2.6 required)'})"
        )
        logger.info(
            f"  databricks-sdk version: {sdk_v} "
            f"({'supported' if sdk_ok else 'NOT supported (>= 0.104.0 required)'})"
        )
```

Wire it into the existing `dbt debug` surface — typically inside the adapter's `debug_query` or wherever the standard connection block prints. Place the call after the host/http_path are known and before the `select 1` execution.

- [ ] **Step 5: Run test to verify pass**

```bash
hatch run unit tests/unit/test_adapter.py::TestDebugSurfacesSpogStatus -v
```

Expected: PASS.

- [ ] **Step 6: Manual sanity check against the live SPOG host**

```bash
DBT_DATABRICKS_HOST_NAME=peco.azuredatabricks.net \
DBT_DATABRICKS_HTTP_PATH="$DBT_DATABRICKS_HTTP_PATH?o=6436897454825492" \
hatch run dbt debug --profiles-dir tests/integration --target uc_endpoint 2>&1 | tee /tmp/spog-debug.log
```

Expected output contains, inline with the standard `dbt debug` block:

```
  SPOG host (host_type='unified'): yes
  workspace_id (from ?o= in http_path): '6436897454825492'
  databricks-sql-connector version: 4.2.6 (supported)
  databricks-sdk version: 0.104.x (supported)
```

- [ ] **Step 7: Commit**

```bash
git add dbt/adapters/databricks/impl.py tests/unit/test_adapter.py
git commit -m "feat(spog): surface SPOG status in dbt debug output

debug_emit_spog_block prints host_type, extracted workspace_id, and
connector/sdk version suitability. Makes 'is SPOG working here?' a
one-command answer for support escalations."
```

---

## Phase 3 — Functional tests on SPOG matrix

### Task 11: Add SPOG profile target

**Files:**
- Modify: `tests/profiles.py`
- Modify: `tests/conftest.py` (if it registers profiles by name)

- [ ] **Step 1: Add SPOG target builder**

In `tests/profiles.py`, append after `databricks_uc_sql_endpoint_target`:

```python
def databricks_uc_sql_endpoint_spog_target():
    """SPOG variant of databricks_uc_sql_endpoint_target.

    Uses the same workspace and credentials as the legacy target. Only the
    host and the ?o= suffix on http_path differ. Driven by:
      DBT_DATABRICKS_SPOG_HOST_NAME (e.g. peco.azuredatabricks.net)
      DBT_DATABRICKS_SPOG_WORKSPACE_ID (e.g. 6436897454825492)
    Falls back to the legacy env vars when those are unset (so the test
    matrix degrades gracefully on developer machines).
    """
    spog_host = os.getenv("DBT_DATABRICKS_SPOG_HOST_NAME")
    spog_ws_id = os.getenv("DBT_DATABRICKS_SPOG_WORKSPACE_ID")
    base = databricks_uc_sql_endpoint_target()
    if spog_host and spog_ws_id:
        base["host"] = spog_host
        base["http_path"] = base["http_path"] + f"?o={spog_ws_id}"
    return base


def get_databricks_cluster_target(profile_type: str):
    if profile_type == "databricks_cluster":
        return databricks_cluster_target()
    elif profile_type == "databricks_uc_cluster":
        return databricks_uc_cluster_target()
    elif profile_type == "databricks_uc_sql_endpoint":
        return databricks_uc_sql_endpoint_target()
    elif profile_type == "databricks_uc_sql_endpoint_spog":
        return databricks_uc_sql_endpoint_spog_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
```

- [ ] **Step 2: Verify the target builder**

Run:

```bash
DBT_DATABRICKS_SPOG_HOST_NAME=peco.azuredatabricks.net \
DBT_DATABRICKS_SPOG_WORKSPACE_ID=6436897454825492 \
hatch run python -c "
from tests.profiles import get_databricks_cluster_target
p = get_databricks_cluster_target('databricks_uc_sql_endpoint_spog')
print(p['host'])
print(p['http_path'])
"
```

Expected:

```
peco.azuredatabricks.net
<your http_path>?o=6436897454825492
```

- [ ] **Step 3: Commit**

```bash
git add tests/profiles.py
git commit -m "test(spog): add databricks_uc_sql_endpoint_spog target builder

Mirrors databricks_uc_sql_endpoint but swaps host to the SPOG vanity URL
and appends ?o=<workspace-id>. Same creds, same workspace. Driven by
DBT_DATABRICKS_SPOG_HOST_NAME and DBT_DATABRICKS_SPOG_WORKSPACE_ID."
```

### Task 12: Functional test — `dbt debug` surfaces SPOG status

**Files:**
- Create: `tests/functional/adapter/spog/__init__.py`
- Create: `tests/functional/adapter/spog/test_spog_debug.py`

- [ ] **Step 1: Scaffold**

```bash
mkdir -p tests/functional/adapter/spog
touch tests/functional/adapter/spog/__init__.py
```

- [ ] **Step 2: Write the test**

Create `tests/functional/adapter/spog/test_spog_debug.py`:

```python
import os

import pytest

from tests.profiles import databricks_uc_sql_endpoint_spog_target

pytestmark = pytest.mark.skipif(
    not (
        os.getenv("DBT_DATABRICKS_SPOG_HOST_NAME")
        and os.getenv("DBT_DATABRICKS_SPOG_WORKSPACE_ID")
    ),
    reason="SPOG env vars not set; skipping SPOG functional tests",
)


class TestSpogDebugOutput:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return databricks_uc_sql_endpoint_spog_target()

    def test_dbt_debug_reports_spog(self, project):
        from dbt.tests.util import run_dbt
        result = run_dbt(["debug"], expect_pass=True)
        # Look for the SPOG block in the captured log
        # (run_dbt captures stdout via the test harness)
        stdout = "\n".join(result.lines_to_print) if hasattr(result, "lines_to_print") else ""
        # Fallback for older test-util versions that put output on .stdout
        if not stdout and hasattr(result, "stdout"):
            stdout = result.stdout
        assert "SPOG host" in stdout
        assert "workspace_id" in stdout
        assert os.environ["DBT_DATABRICKS_SPOG_WORKSPACE_ID"] in stdout
```

- [ ] **Step 3: Run the test**

```bash
DBT_DATABRICKS_SPOG_HOST_NAME=peco.azuredatabricks.net \
DBT_DATABRICKS_SPOG_WORKSPACE_ID=6436897454825492 \
hatch run pytest tests/functional/adapter/spog/test_spog_debug.py -v
```

Expected: PASS — `dbt debug` output contains the SPOG block.

- [ ] **Step 4: Commit**

```bash
git add tests/functional/adapter/spog/__init__.py tests/functional/adapter/spog/test_spog_debug.py
git commit -m "test(spog): functional test for dbt debug SPOG block

Skipped automatically when SPOG env vars are absent (e.g. local dev
without SPOG creds). On the SPOG matrix in CI it asserts the SPOG host
block, extracted workspace_id, and dep-version status all appear."
```

### Task 13: Functional test — missing `?o=` raises pointed error

**Files:**
- Create: `tests/functional/adapter/spog/test_spog_missing_o_raises.py`

- [ ] **Step 1: Write the test**

```python
import os

import pytest
from dbt_common.exceptions import DbtConfigError

from tests.profiles import databricks_uc_sql_endpoint_spog_target

pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_DATABRICKS_SPOG_HOST_NAME"),
    reason="SPOG env vars not set; skipping SPOG functional tests",
)


class TestSpogMissingOraises:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        target = databricks_uc_sql_endpoint_spog_target()
        # Strip ?o=... from http_path to simulate the misconfig
        target["http_path"] = target["http_path"].split("?", 1)[0]
        return target

    def test_dbt_debug_fails_with_pointed_error(self, project):
        from dbt.tests.util import run_dbt
        # expect_pass=False makes the harness not assert success
        with pytest.raises(DbtConfigError) as excinfo:
            run_dbt(["debug"], expect_pass=False)
        msg = str(excinfo.value)
        assert "SPOG" in msg or "unified" in msg
        assert "?o=" in msg
        assert "workspace-id" in msg.lower() or "workspace id" in msg.lower()
```

- [ ] **Step 2: Run the test**

```bash
DBT_DATABRICKS_SPOG_HOST_NAME=peco.azuredatabricks.net \
DBT_DATABRICKS_SPOG_WORKSPACE_ID=6436897454825492 \
hatch run pytest tests/functional/adapter/spog/test_spog_missing_o_raises.py -v
```

Expected: PASS — `DbtConfigError` raised with the §8 row-4 message.

- [ ] **Step 3: Commit**

```bash
git add tests/functional/adapter/spog/test_spog_missing_o_raises.py
git commit -m "test(spog): functional test for missing ?o= error path

Exercises spec §8 row 4: SPOG host detected, http_path lacks ?o=,
adapter raises DbtConfigError naming both ?o= and the workspace-id
field to update."
```

### Task 14: Functional test — probe failure falls back with WARN

**Files:**
- Create: `tests/functional/adapter/spog/test_spog_probe_failure_fallback.py`

- [ ] **Step 1: Write the test**

```python
import os
from unittest import mock

import pytest
import requests

from tests.profiles import databricks_uc_sql_endpoint_target

pytestmark = pytest.mark.skipif(
    not os.getenv("DBT_DATABRICKS_HOST_NAME"),
    reason="Legacy env vars not set; skipping",
)


class TestSpogProbeFailureFallback:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return databricks_uc_sql_endpoint_target()  # legacy host

    def test_probe_failure_warn_and_run_succeeds(self, project, caplog):
        # Force the probe to fail by patching requests.get
        from dbt.tests.util import run_dbt
        with mock.patch(
            "dbt.adapters.databricks.spog.probe.requests.get",
            side_effect=requests.ConnectionError("simulated network failure"),
        ):
            # The run must still succeed — probe failure is non-fatal
            run_dbt(["debug"], expect_pass=True)
        # And we must have logged the WARN
        warn_msgs = [
            r.getMessage()
            for r in caplog.records
            if r.levelname == "WARNING"
        ]
        assert any("SPOG discovery probe" in m for m in warn_msgs), warn_msgs
```

- [ ] **Step 2: Run the test**

```bash
hatch run pytest tests/functional/adapter/spog/test_spog_probe_failure_fallback.py -v
```

Expected: PASS — probe failure is logged as WARN and `dbt debug` still succeeds on the legacy target.

- [ ] **Step 3: Commit**

```bash
git add tests/functional/adapter/spog/test_spog_probe_failure_fallback.py
git commit -m "test(spog): functional test for probe-failure fallback path

Simulates a network failure on the discovery probe and asserts:
  1. The WARN is logged at runtime (visible in dbt output).
  2. The run still succeeds against the legacy host (non-fatal fallback)."
```

### Task 15: CI workflow — add SPOG matrix as a parallel job

**Files:**
- Modify: `.github/workflows/integration.yml` (or whichever workflow runs the integration matrix)

- [ ] **Step 1: Locate the existing integration workflow**

```bash
ls -la .github/workflows/ | grep -i 'integration\|test'
```

Identify the workflow that runs `hatch run pytest tests/functional/...` against the SQL endpoint. Open it.

- [ ] **Step 2: Add a SPOG matrix entry**

Add a parallel job (or matrix entry) that re-runs the same tests with SPOG env vars set. Concretely, near the existing job that runs `databricks_uc_sql_endpoint`:

```yaml
  integration-spog:
    name: Integration (SPOG)
    runs-on: ubuntu-latest
    env:
      # Re-use the same creds and workspace as the legacy matrix
      DBT_DATABRICKS_HOST_NAME: ${{ secrets.DBT_DATABRICKS_HOST_NAME }}
      DBT_DATABRICKS_TOKEN: ${{ secrets.DBT_DATABRICKS_TOKEN }}
      DBT_DATABRICKS_HTTP_PATH: ${{ secrets.DBT_DATABRICKS_HTTP_PATH }}
      DBT_DATABRICKS_UC_INITIAL_CATALOG: ${{ secrets.DBT_DATABRICKS_UC_INITIAL_CATALOG }}
      DBT_DATABRICKS_UC_INITIAL_SCHEMA: ${{ secrets.DBT_DATABRICKS_UC_INITIAL_SCHEMA }}
      # SPOG overrides
      DBT_DATABRICKS_SPOG_HOST_NAME: peco.azuredatabricks.net
      DBT_DATABRICKS_SPOG_WORKSPACE_ID: '6436897454825492'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - name: Install hatch + force SPOG-capable deps
        run: |
          pip install hatch
          # Ensure SPOG-capable versions land in the env
          hatch run pip install 'databricks-sql-connector>=4.2.6,<4.3.0' 'databricks-sdk>=0.104.0,<0.105.0'
      - name: Run functional tests on SPOG target
        run: |
          hatch run pytest tests/functional/adapter --profile databricks_uc_sql_endpoint_spog -v
      - name: Run SPOG-specific tests
        run: |
          hatch run pytest tests/functional/adapter/spog -v
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/integration.yml
git commit -m "ci(spog): add SPOG integration matrix

Parallel job that re-runs the existing functional suite against the
peco.azuredatabricks.net SPOG vanity URL plus the new SPOG-specific
tests. Uses the same Azure workspace and creds as the legacy matrix;
only host and ?o= suffix differ. Pins SPOG-capable connector/sdk."
```

---

## Phase 4 — Wrap-up

### Task 16: Update CHANGELOG and README

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md` (connection-config section, if one exists)

- [ ] **Step 1: CHANGELOG entry**

Add to the top of `CHANGELOG.md` under the unreleased section:

```markdown
## Unreleased

### Features

- Support for SPOG (Single Point of Gateway) hosts. dbt-databricks now correctly routes both data-plane (SQL) and control-plane (REST) traffic when `host` is an account-level vanity URL and `http_path` contains `?o=<workspace-id>`. Opt-in: requires `databricks-sql-connector >= 4.2.6` and `databricks-sdk >= 0.104.0`. See `docs/superpowers/specs/2026-05-19-dbt-databricks-spog-design.md` for design and `.claude/ideas/spog-future-tasks.md` for follow-ups.

### Fixes

- Cluster-id extraction regex now stops at `?` / `&` (previously a greedy capture swallowed any trailing query string into the cluster id; harmless on legacy hosts, latent corruption on SPOG hosts).
```

- [ ] **Step 2: README — add SPOG profile example if there's a connection-config section**

Run:

```bash
grep -n "## Quick" README.md | head
```

If there's a connection example, add a sibling SPOG example. If there isn't, skip (the dbt-docs update tracked in `.claude/ideas/spog-future-tasks.md` will cover this).

- [ ] **Step 3: Run /simplify on the full change set**

Per project conventions ([Memory: ask before commit, /simplify first]):

```bash
# In a fresh worktree session
/simplify
```

Address any simplification suggestions inline. Re-run unit + functional tests if anything changed.

- [ ] **Step 4: Run pre-commit before pushing**

```bash
hatch run pre-commit run --all-files
```

Expected: All hooks pass.

- [ ] **Step 5: Commit and push**

```bash
git add CHANGELOG.md README.md
git commit -m "docs(spog): add changelog and README entries for SPOG support"
git pp
```

- [ ] **Step 6: Open the PR**

Use the conventions from CLAUDE.md (cross-fork via GraphQL is for universe; this is open-source databricks/dbt-databricks where standard `gh pr create` works):

```bash
gh auth switch --user sd-db
gh pr create --title "feat: add SPOG support (opt-in via dep ceiling bumps)" --body "$(cat <<'EOF'
## Summary

Add SPOG (Single Point of Gateway) support to dbt-databricks. Users connecting to account-level vanity URLs (e.g. `peco.azuredatabricks.net`) with `?o=<workspace-id>` in `http_path` get correct routing for both data-plane and control-plane calls.

Design: `docs/superpowers/specs/2026-05-19-dbt-databricks-spog-design.md`
Follow-ups: `.claude/ideas/spog-future-tasks.md`

## What's in this PR

- Bump dep ceilings (`databricks-sql-connector` <4.3.0, `databricks-sdk` <0.105.0); declare `packaging`. Floors unchanged — SPOG is opt-in.
- Fix cluster-id extraction regex (`(.*)` → `([^?&]+)`).
- New `dbt/adapters/databricks/spog/` package: `extract`, `capabilities`, `probe`, `decision`.
- Plumb `Config(workspace_id=...)` into all five `authenticate_with_*` methods.
- Wire `check_spog_preconditions` into `DatabricksConnectionManager.open()`.
- Extend `dbt debug` to surface SPOG status + dep-version suitability.
- New functional tests under `tests/functional/adapter/spog/`.
- New CI matrix that re-runs the existing functional suite against the SPOG target with the same Azure creds.

## Pre-merge verification

Pre-spec probes (§10) run against `peco.azuredatabricks.net?o=6436897454825492` — all four matched expected outcomes (output attached as a comment below).

## Test plan

- [x] Unit tests (`hatch run unit`)
- [x] Functional tests on legacy target (Matrix A)
- [x] Functional tests on SPOG target (Matrix B)
- [x] `hatch run pre-commit run --all-files`

## Follow-ups (separately tracked)

See `.claude/ideas/spog-future-tasks.md` for: dbt-docs update, SPOG-default test profile migration, `dbt debug --verbose-spog`, `1.11.latest` backport of the regex fix, and the eventual deprecation of the capability-detection branch.
EOF
)"
```

---

## Plan self-review

### Spec coverage

| Spec section | Implemented in |
|---|---|
| §4 — auto-derive `workspace_id` | Task 3 (extract) + Task 8 (creds plumbing) |
| §5 — opt-in via ceiling bumps | Task 1 |
| §6 — capability detection | Task 4 |
| §7 — discovery probe + retry/backoff | Tasks 5 + 6 |
| §8 — decision matrix | Task 7 |
| §9.1 — always-on changes | Tasks 1, 2, 3, 4 (Phase 1 group) |
| §9.2 — capability-gated `Config(workspace_id=…)` | Task 8 |
| §9.3 — `dbt debug` SPOG block | Task 10 |
| §10 — pre-spec verification | Pre-implementation gate |
| §11.1 — unit tests | Each Task has its unit tests; the decision-matrix test set covers the §11.1 10-row matrix |
| §11.2 — Matrix A (legacy) | No new work — existing tests stay; Task 15 adds the SPOG matrix beside it |
| §11.2 — Matrix B (SPOG mirror) | Tasks 11, 12, 13, 14, 15 |
| §13 — risks | Probe latency mitigated by Task 5's cache; SDK API drift verified empirically by Matrix A in CI; per-compute mismatch handled in Task 7's `test_multi_compute_conflicting_raises` |
| §15 — future work | Linked to `.claude/ideas/spog-future-tasks.md` in CHANGELOG / PR body |

### Placeholder scan

No "TBD", "TODO", or "similar to Task N" remain. Task 10 (`dbt debug` integration) instructs the implementer to first `grep` for the existing debug surface — this is *not* a placeholder; the codebase has multiple plausible hook points and the implementer needs to pick the right one based on what they find. The actual implementation code (`debug_emit_spog_block`) is fully specified.

### Type consistency

- `HostMetadata` is defined in Task 5 and consumed in Tasks 7, 10. Field names match: `host_type`, `account_id`.
- `extract_workspace_id` signature stable across Tasks 3, 7, 8, 10.
- `check_spog_preconditions(*, host, http_paths)` signature stable across Tasks 7 and 9.
- `DatabricksCredentialManager.workspace_id` is the field name in Tasks 8, 9, 10.
- `connector_supports_spog`, `sdk_supports_workspace_id` consistent across Tasks 4, 7, 10.

No mismatches found.

### Scope check

Single feature (SPOG support). The two-phase rollout (always-on then capability-gated) is a release strategy, not subsystem decomposition — one plan covers both phases. Self-contained.
