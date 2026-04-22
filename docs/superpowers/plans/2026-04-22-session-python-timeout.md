# Session Python Timeout & Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add timeout protection and Spark job group isolation to `SessionPythonSubmitter` so Python models in session mode can be cancelled and don't hang indefinitely.

**Architecture:** Wrap the existing `exec()` call in a daemon thread. Use `sparkContext.setJobGroup()` before execution for isolation, `thread.join(timeout)` for timeout, and `sparkContext.cancelJobGroup()` for cancellation. Pass the timeout from model config through `SessionPythonJobHelper`.

**Tech Stack:** Python threading, PySpark SparkContext job groups, pytest with unittest.mock

**Test command:** `.venv/bin/python -m pytest tests/unit/test_session_python.py -v --no-header`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `dbt/adapters/databricks/python_models/python_submissions.py` | Modify lines 768-843 | Add timeout, threading, job group to `SessionPythonSubmitter` and `SessionPythonJobHelper` |
| `tests/unit/test_session_python.py` | Modify | Add tests for timeout, cancellation, exception propagation, job group |

---

### Task 1: Test and implement timeout on SessionPythonSubmitter

**Files:**
- Modify: `tests/unit/test_session_python.py`
- Modify: `dbt/adapters/databricks/python_models/python_submissions.py:768-813`

- [ ] **Step 1: Write failing test — timeout raises DbtRuntimeError**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonSubmitter`:

```python
def test_submit_raises_on_timeout(self, mock_spark):
    """Test that submit raises DbtRuntimeError when execution exceeds timeout."""
    submitter = SessionPythonSubmitter(mock_spark, timeout=1)
    # Code that sleeps longer than timeout
    compiled_code = "import time; time.sleep(10)"

    with pytest.raises(DbtRuntimeError) as exc_info:
        submitter.submit(compiled_code)

    assert "timed out" in str(exc_info.value)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py::TestSessionPythonSubmitter::test_submit_raises_on_timeout -v --no-header`
Expected: FAIL — `SessionPythonSubmitter.__init__()` does not accept `timeout` parameter

- [ ] **Step 3: Write failing test — successful execution within timeout**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonSubmitter`:

```python
def test_submit_succeeds_within_timeout(self, mock_spark):
    """Test that submit completes normally when execution finishes before timeout."""
    submitter = SessionPythonSubmitter(mock_spark, timeout=10)
    compiled_code = "result = 1 + 1"

    # Should not raise
    submitter.submit(compiled_code)
```

- [ ] **Step 4: Implement timeout in SessionPythonSubmitter**

Modify `dbt/adapters/databricks/python_models/python_submissions.py`. Add `import threading` and `import uuid` to the top of the file (they are not yet imported). Replace the `SessionPythonSubmitter` class (lines 768-813):

```python
class SessionPythonSubmitter(PythonSubmitter):
    """Submitter for Python models using direct execution in current SparkSession.

    Executes compiled code in a daemon thread with timeout protection.
    Uses Spark job groups for isolation so cancellation only affects
    this model's Spark jobs, not other concurrent models.
    """

    def __init__(self, spark: "SparkSession", timeout: int = DEFAULT_TIMEOUT):
        self._spark = spark
        self._timeout = timeout
        self._state_manager = SessionStateManager()

    @override
    def submit(self, compiled_code: str) -> None:
        logger.debug("Executing Python model directly in SparkSession.")

        exec_globals = self._state_manager.get_clean_exec_globals(self._spark)
        group_id = f"dbt-session-python-{uuid.uuid4()}"
        exception_holder: list[BaseException] = []

        def _execute() -> None:
            try:
                self._spark.sparkContext.setJobGroup(
                    group_id,
                    "dbt Python model execution",
                    interruptOnCancel=True,
                )
                exec(compiled_code, exec_globals)
            except Exception as e:
                exception_holder.append(e)

        preview_len = min(500, len(compiled_code))
        logger.debug(
            f"[Session Python] Executing code preview: {compiled_code[:preview_len]}..."
        )

        thread = threading.Thread(target=_execute, daemon=True)
        thread.start()
        thread.join(timeout=self._timeout)

        try:
            if thread.is_alive():
                logger.warning(
                    f"Python model execution timed out after {self._timeout}s, "
                    f"cancelling Spark job group {group_id}"
                )
                self._spark.sparkContext.cancelJobGroup(group_id)
                raise DbtRuntimeError(
                    f"Python model execution timed out after {self._timeout} seconds"
                )

            if exception_holder:
                raise DbtRuntimeError(
                    f"Python model execution failed: {exception_holder[0]}"
                ) from exception_holder[0]

            logger.debug("[Session Python] Model execution completed successfully")

        finally:
            try:
                self._state_manager.cleanup_temp_views(self._spark)
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temp views: {cleanup_error}")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py::TestSessionPythonSubmitter -v --no-header`
Expected: PASS for all tests in this class (including the existing ones which now need the submitter fixture updated — see next step)

- [ ] **Step 6: Update existing submitter fixture to pass timeout**

The existing `submitter` fixture in `TestSessionPythonSubmitter` creates `SessionPythonSubmitter(mock_spark)`. This still works because `timeout` has a default. However, verify all existing tests still pass:

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py -v --no-header`
Expected: All 13 existing tests + 2 new tests PASS

- [ ] **Step 7: Commit**

```bash
git add dbt/adapters/databricks/python_models/python_submissions.py tests/unit/test_session_python.py
git commit -m "feat(session): add timeout and thread-based execution to SessionPythonSubmitter"
```

---

### Task 2: Test and implement Spark job group cancellation

**Files:**
- Modify: `tests/unit/test_session_python.py`
- (Implementation already done in Task 1 — this task adds targeted tests for job group behavior)

- [ ] **Step 1: Write test — setJobGroup is called before execution**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonSubmitter`:

```python
def test_submit_sets_job_group(self, mock_spark):
    """Test that submit sets a Spark job group for isolation."""
    submitter = SessionPythonSubmitter(mock_spark, timeout=10)
    compiled_code = "result = 1"

    submitter.submit(compiled_code)

    mock_spark.sparkContext.setJobGroup.assert_called_once()
    call_args = mock_spark.sparkContext.setJobGroup.call_args
    assert call_args[0][0].startswith("dbt-session-python-")
    assert call_args[1]["interruptOnCancel"] is True
```

- [ ] **Step 2: Write test — cancelJobGroup is called on timeout**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonSubmitter`:

```python
def test_submit_cancels_job_group_on_timeout(self, mock_spark):
    """Test that submit cancels the Spark job group when execution times out."""
    submitter = SessionPythonSubmitter(mock_spark, timeout=1)
    compiled_code = "import time; time.sleep(10)"

    with pytest.raises(DbtRuntimeError):
        submitter.submit(compiled_code)

    mock_spark.sparkContext.cancelJobGroup.assert_called_once()
    group_id = mock_spark.sparkContext.setJobGroup.call_args[0][0]
    mock_spark.sparkContext.cancelJobGroup.assert_called_with(group_id)
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py::TestSessionPythonSubmitter -v --no-header`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_session_python.py
git commit -m "test(session): add job group isolation tests for SessionPythonSubmitter"
```

---

### Task 3: Test and implement exception propagation from exec thread

**Files:**
- Modify: `tests/unit/test_session_python.py`

- [ ] **Step 1: Write test — exception includes original traceback**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonSubmitter`:

```python
def test_submit_propagates_exception_with_cause(self, mock_spark):
    """Test that exceptions from exec thread are re-raised with __cause__ set."""
    submitter = SessionPythonSubmitter(mock_spark, timeout=10)
    compiled_code = "raise ValueError('inner error')"

    with pytest.raises(DbtRuntimeError) as exc_info:
        submitter.submit(compiled_code)

    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert "inner error" in str(exc_info.value.__cause__)
```

- [ ] **Step 2: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py::TestSessionPythonSubmitter::test_submit_propagates_exception_with_cause -v --no-header`
Expected: PASS (implementation already handles this via `from exception_holder[0]`)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_session_python.py
git commit -m "test(session): add exception propagation test for SessionPythonSubmitter"
```

---

### Task 4: Pass timeout from SessionPythonJobHelper to submitter

**Files:**
- Modify: `dbt/adapters/databricks/python_models/python_submissions.py:815-843`
- Modify: `tests/unit/test_session_python.py`

- [ ] **Step 1: Write failing test — helper passes timeout to submitter**

Add to `tests/unit/test_session_python.py` in `TestSessionPythonJobHelper`:

```python
def test_init_passes_timeout_to_submitter(self, mock_credentials, parsed_model_dict):
    """Test that __init__ passes model timeout config to the submitter."""
    parsed_model_dict["config"]["timeout"] = 7200
    mock_spark = MagicMock()
    mock_spark.sparkContext.applicationId = "app-123"
    mock_builder = MagicMock()
    mock_builder.getOrCreate.return_value = mock_spark

    with patch.dict(
        "sys.modules",
        {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
    ):
        import sys

        sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

        helper = SessionPythonJobHelper(parsed_model_dict, mock_credentials)

        assert helper._submitter._timeout == 7200
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py::TestSessionPythonJobHelper::test_init_passes_timeout_to_submitter -v --no-header`
Expected: FAIL — current code creates `SessionPythonSubmitter(self._spark)` without timeout

- [ ] **Step 3: Update SessionPythonJobHelper to pass timeout**

Modify `dbt/adapters/databricks/python_models/python_submissions.py`. Replace the `SessionPythonJobHelper` class (lines 815-843):

```python
class SessionPythonJobHelper(PythonJobHelper):
    """Helper for Python models executing directly in session mode.

    This helper executes Python models directly in the current SparkSession
    without using the Databricks API. It's designed for running dbt on
    job clusters where the SparkSession is already available.
    """

    tracker = PythonRunTracker()

    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.credentials.validate_creds()
        self.parsed_model = ParsedPythonModel(**parsed_model)

        # Get SparkSession directly - no API client needed
        from pyspark.sql import SparkSession

        self._spark = SparkSession.builder.getOrCreate()
        logger.debug(
            f"[Session Python] Using SparkSession: {self._spark.sparkContext.applicationId}"
        )

        self._submitter = SessionPythonSubmitter(
            self._spark, timeout=self.parsed_model.config.timeout
        )

    def submit(self, compiled_code: str) -> None:
        """Submit the compiled Python model for execution."""
        self._submitter.submit(compiled_code)
```

- [ ] **Step 4: Run all tests to verify everything passes**

Run: `.venv/bin/python -m pytest tests/unit/test_session_python.py -v --no-header`
Expected: All tests PASS (13 existing + 6 new = 19 total)

- [ ] **Step 5: Commit**

```bash
git add dbt/adapters/databricks/python_models/python_submissions.py tests/unit/test_session_python.py
git commit -m "feat(session): pass model timeout config to SessionPythonSubmitter"
```
