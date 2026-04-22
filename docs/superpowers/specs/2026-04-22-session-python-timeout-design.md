# Session Python Model Timeout & Cancellation

**Date:** 2026-04-22
**Branch:** `fix/session-python-timeout`
**Status:** Approved

## Problem

`SessionPythonSubmitter.exec()` at `python_submissions.py:799` blocks the calling thread indefinitely when executing Python models in session mode on job clusters. Unlike the Command API and Job Run API submission paths — which have polling loops with configurable timeout (default 24h) — the session path has no timeout, no cancellation support, and no isolation between concurrent models.

On job clusters, the Command Execution API is not available (requires interactive clusters), and `DatabricksApiClient` requires host/token credentials that session mode profiles don't have. So `exec()` is the correct execution model — it just needs hardening to match the robustness of the API-based paths.

## Constraints

- Command API does not work on job clusters (requires interactive clusters)
- `DatabricksApiClient` requires host/token; session mode profiles have `method: session` with no host
- Databricks SDK `Config(host="")` blocks env var auto-detection for runtime-native auth
- Multiple Python models may run concurrently (`threads > 1`), so cancellation must be model-scoped
- The Databricks job has its own outer timeout (e.g., 5 hours), but dbt needs per-model control

## Solution

Wrap `exec()` in a daemon thread with Spark job group isolation, timeout via `thread.join(timeout)`, and cancellation via `sparkContext.cancelJobGroup()`.

## Changes

### `SessionPythonSubmitter` (python_submissions.py)

- Accept `timeout` parameter (from model config, default 24h via `DEFAULT_TIMEOUT`)
- Before `exec()`: call `spark.sparkContext.setJobGroup(group_id, description, interruptOnCancel=True)` using a unique group ID per model execution (UUID-based)
- Run `exec()` in a daemon thread
- Main thread calls `thread.join(timeout)`
- If thread is still alive after join: call `spark.sparkContext.cancelJobGroup(group_id)`, raise `DbtRuntimeError("Python model execution timed out")`
- If thread completed with exception: re-raise as `DbtRuntimeError` with original traceback
- Cleanup (temp views) runs in main thread after join, regardless of outcome

### `SessionPythonJobHelper` (python_submissions.py)

- Read `parsed_model.config.timeout` (already parsed by `ParsedPythonModel`)
- Pass timeout to `SessionPythonSubmitter`

### No changes to

- `SessionStateManager` — stays as-is
- `PythonRunTracker` — session path doesn't use it
- `impl.py` — auto-selection logic stays the same
- Any other submission method
- `run_tracking.py` — has a separate deadlock bug, will be fixed in a different PR

## Data Flow

```
SessionPythonJobHelper.submit(compiled_code)
  → SessionPythonSubmitter.submit(compiled_code)
    → setJobGroup(unique_id, description, interruptOnCancel=True)
    → spawn Thread(target=_execute, args=(compiled_code, exec_globals))
    → thread.join(timeout)
    → if alive: cancelJobGroup(unique_id) → raise timeout error
    → if exception captured: re-raise as DbtRuntimeError
    → cleanup temp views
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Exec raises exception | Captured in thread, re-raised in main thread as `DbtRuntimeError` |
| Timeout exceeded | `cancelJobGroup()` + `DbtRuntimeError("Python model execution timed out")` |
| Cleanup failure | Warning log (same as current behavior) |

## Testing

- Unit test: timeout triggers cancellation and raises error
- Unit test: exception from exec propagates correctly
- Unit test: successful execution completes normally
- Unit test: job group is set before execution and cleaned up after
