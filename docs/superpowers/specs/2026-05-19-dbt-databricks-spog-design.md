# SPOG support for dbt-databricks

**Date**: 2026-05-19
**Owner**: Shubham Dhal
**Status**: Draft — pending pre-spec verification (see §10)

## 1. Summary

Add Single Point of Gateway (SPOG) support to `dbt-databricks`. SPOG replaces per-workspace hostnames (e.g. `adb-7064161269814046.2.azuredatabricks.net`) with account-level vanity URLs (e.g. `peco.azuredatabricks.net`), disambiguating workspaces via a `?o=<workspace-id>` query parameter on the `http_path`. The user-facing change is one profile field: the `host` and `http_path` they paste from the Databricks UI now carry the SPOG host and the `?o=` suffix.

Support is **opt-in via dependency ceiling bumps** — floors stay where they are so existing users see no churn. SPOG only activates when the user has installed a SPOG-capable connector (`databricks-sql-connector ≥ 4.2.6`) and SDK (`databricks-sdk ≥ 0.104.0`), and is connecting to a SPOG (`host_type: unified`) host.

## 2. Background

### 2.1 What SPOG is

SPOG is a Databricks platform change that lets customers expose all workspaces under an account through a single human-readable hostname. The routing layer disambiguates per-request by reading either:

- `?o=<workspace-id>` in the URL path (Thrift / SEA endpoints; the URL form is preserved through the proxy), or
- the `X-Databricks-Org-Id` HTTP header (endpoints where `?o=` cannot ride in the URL — telemetry, feature-flags, REST control-plane).

OAuth endpoints (`/oidc/v1/*`, `/aad/auth`) are intentionally excluded; they reject requests that carry `X-Databricks-Org-Id`.

### 2.2 Upstream status

All wire-level Databricks SQL clients have converged on **URL embedding**: the user passes `?o=` in the connection's `http_path`, the client extracts it and routes accordingly. The SDK uses a configuration field, since it has no `http_path` concept.

| Client | SPOG PR / version | Mechanism |
|---|---|---|
| `databricks-sql-connector` (Python) | [#767](https://github.com/databricks/databricks-sql-python/pull/767), v4.2.6 (2026-04-23) | `Session._extract_spog_headers(http_path)` |
| `databricks-sql-go` | [#347](https://github.com/databricks/databricks-sql-go/pull/347) | `extractSpogHeaders` + `headerInjectingTransport` |
| `databricks-jdbc` | [#1316](https://github.com/databricks/databricks-jdbc/pull/1316) | `URIBuilder` + `customHeaders` |
| ADBC (Rust) | [#379](https://github.com/adbc-drivers/databricks/pull/379) | parse `?o=` from HTTP path |
| `databricks-sdk-py` | v0.103.0 (2026-04-20), v0.104.0 (2026-04-23) | `Config(workspace_id=...)` / `DATABRICKS_WORKSPACE_ID` env |

### 2.3 Why dbt-databricks needs this

`dbt-databricks` is the only Databricks connector in the partner-ecosystem rollout that bridges both planes:

- **Data plane** — `databricks-sql-connector` for query execution.
- **Control plane** — `databricks-sdk-py` (`WorkspaceClient`) for Python-model submission, cluster start/stop, notebook upload, etc.

Both must understand the user is targeting a SPOG host. The convention (URL embedding) covers the data plane natively; we need to bridge the SPOG context to the SDK ourselves.

### 2.4 Friction in current adapter code (verified in code, paths cited)

1. **Connector pin is below SPOG version.** `pyproject.toml`: `databricks-sql-connector[pyarrow]>=4.1.1, <4.1.6`. PR #767 lives in v4.2.6.
2. **SDK ceiling is below SPOG version.** `pyproject.toml`: `databricks-sdk>=0.68.0, <0.78.0`. SPOG support is in v0.103.0+.
3. **Cluster-ID extraction regex is greedy.** `credentials.py:21`: `re.compile(r"/?sql/protocolv1/o/\d+/(.*)")`. `(.*)` swallows the `?o=…` query string into the cluster ID. PR #767 fixed the equivalent regex in the connector (`.+` → `[^?&]+`); we have the same latent bug.
4. **`Config(...)` calls do not set `workspace_id`.** `credentials.py:295-331` constructs `Config(host=…, token=…)` etc. in five `authenticate_with_*` methods. On SPOG, REST calls from `WorkspaceClient` (Python models, Workspace API, cluster ops) hit the SPOG host with no org-id header.

`is_cluster_http_path` in `utils.py:94` uses substring checks and is unaffected by `?o=`. `validate_creds` does not reject query strings in `http_path`. The five `connection_parameters` that reserve `server_hostname`/`http_path`/etc. (`credentials.py:106`) are unrelated.

## 3. Goals and non-goals

### Goals

- Users connecting to SPOG hosts can run `dbt debug`, `dbt run`, `dbt test`, `dbt source freshness`, and Python models against the four auth modes the adapter supports today: PAT (`token`), OAuth M2M (`client_id` + `client_secret`), OAuth U2M (external-browser; triggered when `auth_type: oauth` without a secret), and Azure AD M2M (`azure_client_id` + `azure_client_secret`). The XTA design doc lists "Azure AD U2M" as a separate mode but the adapter has no Azure-specific U2M code path — Azure U2M users go through `authenticate_with_external_browser`, same as generic OAuth U2M, so it falls out of OAuth U2M coverage.
- Existing (non-SPOG) users see no behavior change and no required dep upgrades.
- When users hit a misconfiguration (SPOG host + missing `?o=`, SPOG host + old deps, non-SPOG host + `?o=`), they get a single, actionable error that names the file/field to fix.
- `dbt debug` surfaces SPOG status and dep-version suitability without needing to grep logs.

### Non-goals

- Solving the SPOG infra issues called out in the design doc as belonging to other teams (DECO M2M `iss` mismatch, `oauth token exchange/extend`, custom OIDC endpoint exposure). Those are tracked separately.
- Adding a new top-level profile field (e.g. `workspace_id`). The `?o=` in `http_path` is the single source of truth, matching every other SQL client.
- Probing or auto-discovering SPOG hosts during `dbt parse` / `dbt list` / `dbt compile` (parse-time flows that have historically not opened warehouse connections).
- Bumping the floor of either dependency. This is a deliberate opt-in posture (§5).

## 4. Approach: derive `workspace_id` from `http_path?o=…`

Single source of truth in the profile is the existing `http_path` field. When it contains `?o=<id>`, we extract `<id>` and:

- Pass `http_path` unchanged to `dbsql.connect(...)`. The connector (≥ 4.2.6) extracts `?o=` itself and injects `X-Databricks-Org-Id` on non-OAuth endpoints.
- Pass the extracted id as `workspace_id=<id>` into every `Config(...)` construction in `DatabricksCredentialManager`. The SDK (≥ 0.103.0) uses it to set `X-Databricks-Org-Id` on `WorkspaceClient` calls when `host_type == UNIFIED`.

Why this and not an explicit `workspace_id` profile field: the connector ecosystem (Python, Go, JDBC, ADBC) has converged on URL embedding because users copy-paste the `http_path` straight from the Databricks UI, which already carries `?o=` for SPOG workspaces. A profile field would duplicate information and create conflict-resolution surface for no benefit to the user.

## 5. Opt-in via ceiling bumps

Floors stay; ceilings move to allow newer SPOG-capable versions to install:

```diff
- databricks-sdk>=0.68.0, <0.78.0
+ databricks-sdk>=0.68.0, <0.105.0
- databricks-sql-connector[pyarrow]>=4.1.1, <4.1.6
+ databricks-sql-connector[pyarrow]>=4.1.1, <4.3.0
+ packaging>=23.0
```

A user pinned at the floor (`databricks-sql-connector==4.1.x`, `databricks-sdk==0.68.x`) continues to work exactly as today against non-SPOG hosts. A user who wants SPOG bumps their own pin and our runtime detection (§6) lights up the SPOG code path.

Rationale: the SPOG-enabled versions of both deps are <4 weeks old as of this spec. We don't want to force every existing user onto a fresh-baked dep just to upgrade `dbt-databricks`. Once those deps stabilize in customer environments (weeks/months), a future PR can bump the floor.

**Why `packaging`:** robust version comparison across alphas/post-releases/dev versions. `tuple(map(int, v.split('.')))` breaks on `4.2.6a1` or `0.104.0.post1`. `packaging` is already a transitive dep of `dbt-core`, so net new-install footprint is zero — we just declare it explicitly so we don't depend on transitive presence.

## 6. Capability detection

Two checks, performed once per process and cached:

```python
# dbt/adapters/databricks/spog/capabilities.py

from importlib.metadata import version as _pkg_version
from importlib.metadata import PackageNotFoundError
import inspect
from functools import cache

from packaging.version import Version

from databricks.sdk.core import Config

_CONNECTOR_SPOG_MIN = Version("4.2.6")


@cache
def connector_supports_spog() -> bool:
    try:
        return Version(_pkg_version("databricks-sql-connector")) >= _CONNECTOR_SPOG_MIN
    except PackageNotFoundError:
        return False


@cache
def sdk_supports_workspace_id() -> bool:
    # Feature-detect: workspace_id became a Config attribute in v0.103.0.
    # Survives forks/wrappers more reliably than version-comparing the SDK.
    return "workspace_id" in inspect.signature(Config).parameters
```

The SDK check uses `inspect.signature` because the SDK is more prone to forking/wrapping in enterprise envs than the connector, and `workspace_id` is a public Config attribute we can observe directly.

The connector check uses version comparison because `dbsql.connect` is a kwargs-bag function with no introspectable signature surface for SPOG. We could `hasattr(Session, '_extract_spog_headers')` but that depends on an internal API.

## 7. Discovery probe

`/.well-known/databricks-config` returns a JSON document with `host_type`. SPOG hosts return `host_type: "unified"`. We probe to drive the §8 decision matrix.

```python
# dbt/adapters/databricks/spog/probe.py

from dataclasses import dataclass
from functools import cache
from typing import Optional

import requests


@dataclass(frozen=True)
class HostMetadata:
    host_type: Optional[str]  # "unified" → SPOG; else legacy / unknown
    account_id: Optional[str]


@cache
def probe_host(host: str) -> HostMetadata:
    """One-shot probe per host per process. Cached for the lifetime of the run.

    Retries with exponential backoff (3 attempts: ~0.5s, ~1s, ~2s with jitter)
    before giving up. A genuine SPOG host's discovery endpoint is highly
    available; persistent failure across three attempts is a strong signal of
    either a non-SPOG host (which doesn't serve this endpoint) or a real
    network problem. In either case we fall back to the legacy code path,
    but we WARN loudly so the user sees it in their `dbt run` output.
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
                # 0.5s, 1s base + jitter; bounded ~2s total worst case
                time.sleep((0.5 * (2 ** attempt)) + random.random() * 0.25)

    logger.warning(
        f"SPOG discovery probe to {url!r} failed after 3 attempts "
        f"(last error: {last_exc}). Proceeding as a non-SPOG host. "
        f"If {host} is a SPOG (unified) workspace, routing errors may follow; "
        f"verify network reachability to /.well-known/databricks-config."
    )
    return HostMetadata(host_type=None, account_id=None)
```

**Where it is called**: only from `DatabricksConnectionManager.open()`. Never from `__init__`, credential creation, capability registration, or `set_connection_name`. This keeps `dbt parse` / `dbt list` / `dbt compile` probe-free, as today.

Probe cost on the happy path: one HTTP GET per host per process (~50-100ms). Worst case (full retry exhaust): ~2s before legacy fallback + WARN. Both are trivial relative to a `dbt run`.

**Defensive posture**: the probe's *failure* mode is "assume non-SPOG and proceed" — never "block the run". The WARN is the signal; routing errors that surface afterward (if the user was actually on a SPOG host the probe couldn't reach) carry the SPOG-team's own `x-databricks-popp-routing-reason: workspace-id` header, which we map to a pointed error per §8 row 5.

## 8. Decision matrix at `connection.open()`

After extracting `?o=` from `http_path` and probing the host:

| host_type | `?o=` extracted | Connector ≥ 4.2.6 | SDK has `workspace_id` | Behavior |
|---|---|---|---|---|
| `unified` | yes | yes | yes | **SPOG path**: pass `http_path` to connector verbatim; set `Config(workspace_id=<id>)` for all SDK calls. |
| `unified` | yes | yes | **no** | `DbtConfigError`: "Host `{host}` is a SPOG (unified) workspace. `databricks-sdk` version `{v}` lacks `workspace_id` support. Upgrade: `pip install 'databricks-sdk>=0.104.0'`." |
| `unified` | yes | **no** | yes | `DbtConfigError`: "Host `{host}` is a SPOG (unified) workspace. `databricks-sql-connector` version `{v}` lacks SPOG support. Upgrade: `pip install 'databricks-sql-connector>=4.2.6'`." |
| `unified` | **no** | any | any | `DbtConfigError`: "Host `{host}` is a SPOG (unified) workspace, but `http_path` does not include `?o=<workspace-id>`. Update your `profiles.yml` to: `http_path: /sql/1.0/warehouses/<id>?o=<workspace-id>`. The workspace ID is visible in the Databricks UI when copying the JDBC/ODBC connection details." |
| not `unified` (probe returned something else) | **yes** | any | any | `DbtConfigError`: "`http_path` contains `?o=<id>` but host `{host}` is not a SPOG workspace (host_type=`{host_type}`). Either change `host` to the SPOG hostname for this workspace, or remove `?o=` from `http_path`." |
| probe failed / unknown | yes | any | any | **Permissive**: log a `warning` "Could not determine SPOG status of `{host}` (discovery probe failed); proceeding with `?o=<id>` in http_path. If you hit routing errors, verify your host is SPOG-enabled." Pass through unchanged. |
| not `unified` / unknown | no | any | any | **Legacy path**, unchanged. No probe-driven branching; no `workspace_id` set on `Config`. |

All "bad" rows hard-fail rather than degrade silently. Pointed errors per the (3) refinement.

When multiple computes are configured (`creds.compute`), each compute's `http_path` is extracted independently. If they disagree (e.g. one has `?o=A`, another has `?o=B`, or one has `?o=` and another doesn't), that is a `DbtConfigError` at validation time — we won't try to reconcile.

## 9. Detailed code changes

### 9.1 Always-on (not capability-gated)

**`pyproject.toml`** — bump ceilings, add `packaging`:

```diff
 dependencies = [
     "click>=8.2.0, <9.0.0",
-    "databricks-sdk>=0.68.0, <0.78.0",
-    "databricks-sql-connector[pyarrow]>=4.1.1, <4.1.6",
+    "databricks-sdk>=0.68.0, <0.105.0",
+    "databricks-sql-connector[pyarrow]>=4.1.1, <4.3.0",
+    "packaging>=23.0",
     ...
 ]
```

**`credentials.py:21`** — fix cluster-ID extraction regex:

```diff
-EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/(.*)")
+EXTRACT_CLUSTER_ID_FROM_HTTP_PATH_REGEX = re.compile(r"/?sql/protocolv1/o/\d+/([^?&]+)")
```

This is a correctness fix; on legacy hosts it's a no-op (no `?` to encounter). On SPOG with cluster compute, it stops the capture at the query string so `creds.cluster_id` is `0605-142813-rf81cyrh` instead of `0605-142813-rf81cyrh?o=6436897454825492`.

**`dbt/adapters/databricks/spog/extract.py`** (new) — `?o=` extraction helper:

```python
from typing import Optional
from urllib.parse import parse_qs, urlparse


def extract_workspace_id(http_path: Optional[str]) -> Optional[str]:
    """Parse ?o=<workspace-id> from a Databricks http_path.

    Returns the workspace id as a string, or None when the http_path has no
    ?o= parameter (or is empty/None). Multi-valued ?o=a&o=b returns the first.
    """
    if not http_path or "?" not in http_path:
        return None
    query = http_path.split("?", 1)[1]
    return parse_qs(query).get("o", [None])[0]
```

Pure function, no I/O. Used by both the data-plane wiring and the SDK-config wiring.

### 9.2 Capability-gated

**`credentials.py` `DatabricksCredentialManager.authenticate_with_*`** — plumb `workspace_id` into `Config(...)` when (a) we successfully extracted one and (b) the SDK supports it.

Sketch (PAT case shown; same shape for the other four):

```python
def authenticate_with_pat(self) -> Config:
    kwargs: dict[str, Any] = {"host": self.host, "token": self.token}
    if self._workspace_id and sdk_supports_workspace_id():
        kwargs["workspace_id"] = self._workspace_id
    return Config(**kwargs)
```

`self._workspace_id` is set in `create_from(...)` by calling `extract_workspace_id(credentials.http_path)`.

**`connections.py` `DatabricksConnectionManager.open()`** — perform the probe, evaluate the decision matrix, raise the appropriate error or fall through to legacy / SPOG path. Concretely: insert a new helper `_check_spog_preconditions(creds, http_path)` that returns silently on the two acceptable paths (SPOG-on, legacy) and raises `DbtConfigError` with the targeted message otherwise.

### 9.3 `dbt debug` integration

`DatabricksAdapter.debug_query()` already runs a trivial `select 1`. We extend the diagnostics emitted around it. After resolving the host and `http_path`, log:

```
  Connecting to Databricks
    host:                                       peco.azuredatabricks.net
    SPOG host (host_type=unified):              yes
    workspace_id (from ?o= in http_path):       6436897454825492
    databricks-sql-connector version:           4.2.6   (SPOG: supported)
    databricks-sdk version:                     0.104.1 (SPOG: supported)
  Connection test: OK
```

On a misconfiguration, the matching diagnostic line marks `(SPOG: NOT supported — upgrade to >= 4.2.6)` etc., immediately before the `DbtConfigError` is raised. This is the canonical place users look when something is wrong, so making the version state explicit here is high-value for ~30 lines of code.

## 10. Pre-spec verification (hard prerequisite)

Before any code changes land, manually verify the upstream libraries do what their PRs claim, using the same Azure test workspace and credentials we already use for CI. Performed in a worktree with fresh installs of the SPOG-capable deps.

Test URL: `https://peco.azuredatabricks.net/?o=6436897454825492` — the SPOG variant of the existing Azure test workspace.

| Probe | Command | Expected |
|---|---|---|
| 1. Discovery endpoint reachable | `curl -sf https://peco.azuredatabricks.net/.well-known/databricks-config` | HTTP 200 with `host_type: "unified"` |
| 2. SDK control-plane (with `workspace_id`) | `databricks workspace list / --host https://peco.azuredatabricks.net --token $DBT_DATABRICKS_TOKEN` after setting `DATABRICKS_WORKSPACE_ID=6436897454825492` | Lists workspace contents (no auth/routing error). SDK ≥ 0.104. |
| 3. Connector data-plane (with `?o=`) | Python: `dbsql.connect(server_hostname='peco.azuredatabricks.net', http_path='$DBT_DATABRICKS_HTTP_PATH?o=6436897454825492', access_token=$DBT_DATABRICKS_TOKEN)` then `SELECT 1` | Returns `1`. Connector ≥ 4.2.6. |
| 4. Negative case for error mapping | Same as (3) but **without** `?o=` | Connection or query fails with response carrying `x-databricks-popp-routing-reason: workspace-id`. We capture the exact failure surface to map to our `DbtConfigError` text. |

Probe (4)'s "pass" condition is observing the expected failure with the `x-databricks-popp-routing-reason: workspace-id` header — that confirms the error surface we'll map to our `DbtConfigError`. If (1), (2), or (3) does not match its expected outcome, design does not ship — we triage with the SPOG team first. If (4) returns a *successful* response (the proxy permitted the call despite the missing `?o=`), the SPOG host is being more permissive than the design doc implies, and we'll re-evaluate whether the always-on probe is justified.

## 11. Testing plan

### 11.1 Unit

- `extract_workspace_id` — happy path, empty/None, no `?o=`, multi-`o`, multi-key query, malformed query.
- Cluster-ID regex — verify capture stops at `?` for both warehouse and cluster `http_path` shapes.
- `connector_supports_spog()` and `sdk_supports_workspace_id()` — mock `importlib.metadata.version` and `inspect.signature(Config)` to exercise both branches.
- `probe_host` — mock `requests.get` to exercise: (a) immediate success with `unified`, (b) immediate success with non-unified, (c) one transient failure then success (retry covers it), (d) three failures → fall back to `HostMetadata(host_type=None)` + WARN log assertion.
- `_check_spog_preconditions` — every row of the §8 matrix has a test asserting either no-raise or the precise `DbtConfigError` message.

**Full old/new dep × SPOG/non-SPOG matrix.** Every combination of inputs that drives a different code path must have a unit test. Concretely:

  | Connector supports SPOG | SDK supports `workspace_id` | host_type from probe | `?o=` in http_path | Test asserts |
  |---|---|---|---|---|
  | yes | yes | unified | yes | `Config(...)` called with `workspace_id=<id>`; `http_path` passed verbatim to `dbsql.connect`; no error |
  | yes | yes | unified | no | `DbtConfigError` mentioning `?o=` |
  | yes | **no** | unified | yes | `DbtConfigError` mentioning `databricks-sdk` upgrade |
  | **no** | yes | unified | yes | `DbtConfigError` mentioning `databricks-sql-connector` upgrade |
  | **no** | **no** | unified | yes | `DbtConfigError` mentioning the lower of the two (or both); deterministic message |
  | yes | yes | not-unified | yes | `DbtConfigError` mentioning hostname mismatch |
  | yes | yes | not-unified | no | Legacy path: `Config(...)` called *without* `workspace_id`; `http_path` passes through |
  | **no** | **no** | not-unified | no | Legacy path same as above (this is the dominant existing user) |
  | yes | yes | probe-failed | yes | WARN logged; legacy path proceeds (per §7 defensive posture) |
  | yes | yes | probe-failed | no | WARN logged; legacy path proceeds |

  We inject the capability-detector return values via `mock.patch` rather than messing with installed versions, so the same test file runs identically under Matrix A and Matrix B (§11.2).

- `DatabricksCredentialManager.authenticate_with_*` (4 methods: PAT, oauth-m2m, external-browser, azure-client-secret) — for each, assert `Config` receives `workspace_id` iff both extraction and `sdk_supports_workspace_id()` return true.

### 11.2 Functional / integration

Two matrices in CI, both must pass before merge. The same Azure test workspace and the same `DBT_DATABRICKS_TOKEN` are used for both — only `host` and the `?o=` suffix on `http_path` differ.

**Matrix A — legacy unchanged**

- Pin: `databricks-sql-connector==4.1.1` (floor), `databricks-sdk==0.68.0` (floor).
- Target: existing (non-SPOG) Azure UC SQL endpoint via existing `DBT_DATABRICKS_*` env.
- Suite: current `tests/functional/adapter/**` plus `tests/integration/**`.
- Asserts: no regressions; probe is never called for these targets (no `.well-known/databricks-config` traffic appears in test logs).

**Matrix B — SPOG mirror**

- Pin: `databricks-sql-connector>=4.2.6,<4.3.0`, `databricks-sdk>=0.104.0,<0.105.0`.
- Target: `peco.azuredatabricks.net` with `http_path=<existing>?o=6436897454825492`, same `DBT_DATABRICKS_TOKEN`.
- Suite: **the entire** functional/integration suite, parametrized to run once against the legacy profile (already covered by Matrix A) and once against the SPOG profile. Because the credentials and underlying workspace are identical, the test code requires zero changes — we just add a second `target:` in `test.dbt-databricks.yml` and a CI job that selects it.
- Plus a small `tests/functional/adapter/spog/` set for SPOG-specific behavior:
  - `test_debug_surfaces_spog_status` (assert `dbt debug` output contains workspace_id and dep-version lines)
  - `test_python_model_runs_on_spog` (smoke test the WorkspaceClient/Jobs code path)
  - `test_missing_o_param_raises` (remove `?o=`; assert the §8 row-4 message)
  - `test_probe_failure_falls_back_to_legacy_with_warn` (block `/.well-known/databricks-config` at the network layer; assert WARN logged and run still succeeds against the test target)

Auth modes supported by the adapter: PAT, OAuth M2M, OAuth U2M (external-browser), Azure AD M2M. Matrix B runs PAT in CI; the three non-PAT modes are gated behind a manually triggered workflow in `.github/workflows/spog-oauth-matrix.yml` (interactive U2M is unsuitable for unattended CI; M2M variants need separate SPNs that don't live in shared CI secrets).

**Future direction (tracked in §15): swap A and B.** Long-term, once SPOG is the universal Databricks routing posture, the default test target moves to the SPOG profile and the *legacy* profile becomes the targeted matrix (specific tests that verify the non-SPOG code path still works for users pinned to old deps). At that point, Matrix A and B reverse their roles. We are not doing this in the initial PRs — too aggressive while SPOG is new — but the test code is structured so that the swap is a one-line target change.

### 11.3 What is not tested

The OAuth-token-exchange/extend failures called out in the design doc (Failed across every other PECO client). These are a known SPOG/Auth team responsibility and out of scope for this change.

## 12. Rollout

1. Land the pre-spec verification commands and capture their output in a worktree (no code changes yet).
2. Open a PR with the always-on changes (§9.1) only. This is a tiny, safe diff: regex fix, helper module, dep ceiling bumps, packaging dep. Existing tests pass unchanged.
3. Open a second PR with the capability-gated changes (§9.2), `dbt debug` extensions (§9.3), probe wiring, decision-matrix enforcement, and the SPOG test matrix.
4. Update `CHANGELOG.md` and the `README.md` connection section with a SPOG profile example:

   ```yaml
   my_project:
     target: prod
     outputs:
       prod:
         type: databricks
         host: peco.azuredatabricks.net
         http_path: /sql/1.0/warehouses/<warehouse-id>?o=<workspace-id>
         token: "{{ env_var('DATABRICKS_TOKEN') }}"
         catalog: main
         schema: analytics
   ```

5. The capability-detection plumbing is **kept while SPOG and non-SPOG hosts coexist** — customers migrate on their own schedule, and dbt-databricks needs to serve both populations during the transition window. Eventual deprecation (once SPOG is universal) is tracked separately and is not part of this initiative; see `.claude/ideas/spog-deprecate-capability-detection.md` for the trigger criteria and the breaking-change framing it will need. Floors will only move as part of that future change.

## 13. Risks

- **Probe failure modes.** If `/.well-known/databricks-config` is briefly unavailable (network blip, proxy issue), we treat the host as non-SPOG and fall through to legacy. On a real SPOG host this means the user gets an opaque routing error from the SDK/connector instead of our pointed message. Mitigation: probe uses a short timeout (5s) and is cached on first success per process, so transient failures only affect the very first connection of a run.
- **SDK API drift between v0.68 and v0.104.** `api_client.py` calls into `clusters.{get,start}`, `command_execution.{create,execute,cancel,destroy}`, `workspace.{mkdirs,import_,get_status}`, `libraries.cluster_status`, `current_user.me`. A spot-check of the SDK changelog between 0.68 and 0.104 shows no breaking changes to these surfaces, but matrix A confirms this empirically by running with floor-pinned deps.
- **Per-compute `http_path` mismatch.** If `creds.compute` defines multiple computes with conflicting `?o=` values (or some with and some without), we fail at validation rather than try to reconcile. This is an unusual config but worth surfacing — the alternative (silently picking one) is worse.
- **`?o=` in `http_path` on legacy hosts** (row 5 of §8). Hard error rather than silent strip, per the (3) refinement. The risk is that during a migration window, a user might paste a SPOG-shaped `http_path` against a still-legacy `host` and get an error they don't immediately understand. Mitigation: the error message names both fields explicitly and offers two resolutions.

## 14. Open items (resolved before merge)

- [ ] Confirm `https://peco.azuredatabricks.net/?o=6436897454825492` resolves to the same Azure workspace currently used by the dbt-databricks CI. Confirmed informally — need a quick `dbt run` against both to verify catalog/schema visibility matches.
- [ ] Run the four pre-spec probes (§10) and attach output to the implementation PR's description.
- [ ] Determine whether `packaging` is already a direct dep of `dbt-core` (transitive availability is confirmed; the question is whether we still want to declare it explicitly). Default: yes, declare explicitly.

## 15. Future work

All follow-up items spawned by this design are tracked in a single file at `.claude/ideas/spog-future-tasks.md` (project-level location for tracked-but-deferred work, shared across worktrees via symlink). The implementation PR's description will link there. The file contains five sections:

1. **dbt-docs update** (separate repo) — push the SPOG profile shape and dep-version requirements into the `dbt-docs` repo's `databricks-setup.md` page.
2. **Migrate default CI test profile to SPOG** — once SPOG is universal, swap which CI matrix is the default test target (Matrix B → default, Matrix A → targeted).
3. **`dbt debug --verbose-spog`** — a verbose-mode flag that dumps the full discovery response and `Config(...)`/`dbsql.connect(...)` kwargs for customer triage.
4. **Backport the cluster-id regex fix to `1.11.latest`** — independently useful correctness improvement for users on the previous minor.
5. **Deprecate the capability-detection branch** — eventual removal of the legacy code path once SPOG is universally adopted (per the comment-8 nuance: kept now, retired later via a breaking-change minor).

Each section is self-contained — context, scope, trigger criteria, risks — so any item can be picked up months from now without re-deriving the rationale.
