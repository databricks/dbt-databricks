# Implementation Plan v2: Full Job Cluster Support for SQL + Python Models

## Summary

Enable complete dbt pipelines (SQL + Python models) to execute entirely on Databricks job clusters by:
1. Adding `method: session` for SQL models using the active SparkSession
2. Adding `session` submission method for Python models executing directly in the same session

**Key Benefits:**
- 🎯 **Primary Goal Achieved**: Run full dbt pipeline (SQL + Python) on a single job cluster
- 💰 **70%+ Cost Savings**: Job clusters cost ~7¢ less than SQL Warehouses on Azure
- 📊 **Better Observability**: Entire dbt run tracked in Databricks Jobs UI (vs. no tracking for SQL models before)
- ⚡ **Faster Execution**: No job submission overhead for Python models
- ✅ **Correct Behavior**: Sequential execution respecting DAG dependencies (as intended)
- 🔧 **Simpler Setup**: No API authentication or permissions management needed

## Background

**Current State:**
- SQL models require DBSQL connector (SQL Warehouses or All-Purpose clusters)
- Python models can use job clusters via Jobs API (but spawn separate jobs)
- No way to run a mixed pipeline entirely within a single job cluster session

**Goal:**
- Run complete dbt pipelines (SQL + Python) on a single job cluster
- All models execute in the same SparkSession
- Preserve all dbt-databricks features (Unity Catalog, Streaming Tables, etc.)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Job Cluster                        │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    SparkSession                           │   │
│  │                                                           │   │
│  │   SQL Models ──────► spark.sql()                          │   │
│  │                                                           │   │
│  │   Python Models ───► exec() with spark context            │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  profiles.yml: method: session                                   │
│  model config: submission_method: session                        │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Approach

### Phase 1: Add Session Connection Method (SQL Models)

**File: [credentials.py](dbt/adapters/databricks/credentials.py)**

1. Add `method` field to `DatabricksCredentials`:
```python
@dataclass
class DatabricksCredentials(Credentials):
    # ... existing fields ...
    method: Optional[str] = None  # "session" or "dbsql" (default)
```

2. Update validation in `__post_init__`:
```python
# Auto-detect session mode
if self.method is None:
    if os.getenv("DBT_DATABRICKS_SESSION_MODE", "").lower() == "true":
        self.method = "session"
    elif os.getenv("DATABRICKS_RUNTIME_VERSION") and not self.host:
        self.method = "session"
    else:
        self.method = "dbsql"

# Validate based on method
if self.method == "session":
    self._validate_session_mode()
else:
    self.validate_creds()
```

3. Add session validation:
```python
def _validate_session_mode(self) -> None:
    try:
        from pyspark.sql import SparkSession  # noqa
    except ImportError:
        raise DbtRuntimeError("Session mode requires pyspark")
    if self.schema is None:
        raise DbtValidationError("Schema is required for session mode")
```

### Phase 2: Create Session Handle Module (SQL Execution)

**New File: [session.py](dbt/adapters/databricks/session.py)**

1. `SessionCursorWrapper` - Adapts DataFrame to cursor interface:
```python
class SessionCursorWrapper:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._df: Optional[DataFrame] = None
        self._rows: Optional[list[Row]] = None

    def execute(self, sql: str, bindings=None) -> "SessionCursorWrapper":
        cleaned_sql = sql.strip().rstrip(";")
        if bindings:
            cleaned_sql = cleaned_sql % tuple(bindings)
        self._df = self._spark.sql(cleaned_sql)
        return self

    def fetchall(self) -> list[tuple]:
        if self._rows is None and self._df:
            self._rows = self._df.collect()
        return [tuple(row) for row in (self._rows or [])]

    @property
    def description(self) -> list[tuple]:
        if self._df is None:
            return []
        return [(f.name, f.dataType.simpleString(), None, None, None, None, f.nullable)
                for f in self._df.schema.fields]
```

2. `DatabricksSessionHandle` - Wraps SparkSession:
```python
class DatabricksSessionHandle:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    @staticmethod
    def create(catalog=None, schema=None, session_properties=None):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        if catalog:
            spark.catalog.setCurrentCatalog(catalog)
        if schema:
            spark.catalog.setCurrentDatabase(schema)
        if session_properties:
            for k, v in session_properties.items():
                spark.conf.set(k, v)
        return DatabricksSessionHandle(spark)

    @property
    def dbr_version(self) -> tuple[int, int]:
        version_str = self._spark.conf.get(
            "spark.databricks.clusterUsageTags.sparkVersion"
        )
        return SqlUtils.extract_dbr_version(version_str)

    def execute(self, sql, bindings=None) -> SessionCursorWrapper:
        return SessionCursorWrapper(self._spark).execute(sql, bindings)
```

### Phase 3: Update Connection Manager

**File: [connections.py](dbt/adapters/databricks/connections.py)**

1. Modify `open()` to dispatch based on method:
```python
@classmethod
def open(cls, connection: Connection) -> Connection:
    creds: DatabricksCredentials = connection.credentials

    if creds.method == "session":
        return cls._open_session(connection, creds, databricks_connection)

    # Existing DBSQL logic...
```

2. Add `_open_session()` method for session-based connections.

### Phase 4: Add Session Python Submission Helper (Python Models)

**File: [python_submissions.py](dbt/adapters/databricks/python_models/python_submissions.py)**

1. Add new `SessionPythonSubmitter`:
```python
class SessionPythonSubmitter(PythonSubmitter):
    """Submitter for Python models using direct execution in current SparkSession.

    NOTE: This does NOT collect data to the driver. The compiled code contains
    df.write.saveAsTable() which writes directly to storage, just like API-based
    submission methods.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    @override
    def submit(self, compiled_code: str) -> None:
        logger.debug("Executing Python model directly in SparkSession.")

        # Create execution context with spark available
        # The compiled code will:
        # 1. Execute model() function to get a DataFrame
        # 2. Call df.write.saveAsTable() to persist to Delta
        # 3. No collect() - data stays distributed
        exec_globals = {
            "spark": self._spark,
            "dbt": __import__("dbt"),
        }

        # Execute the compiled Python model code
        exec(compiled_code, exec_globals)
```

2. Add session state cleanup utilities:
```python
class SessionStateManager:
    """Manages session state to prevent leakage between Python models."""

    @staticmethod
    def cleanup_temp_views(spark: SparkSession) -> None:
        """Drop temporary views created during model execution."""
        # Get list of temp views
        temp_views = [row.name for row in spark.sql("SHOW VIEWS").collect()
                      if row.isTemporary]
        for view in temp_views:
            spark.catalog.dropTempView(view)

    @staticmethod
    def get_clean_exec_globals(spark: SparkSession) -> dict:
        """Return a clean execution context with minimal state."""
        return {
            "spark": spark,
            "dbt": __import__("dbt"),
            # Add other safe imports as needed
        }
```

3. Update `SessionPythonSubmitter` with error handling:
```python
class SessionPythonSubmitter(PythonSubmitter):
    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._state_manager = SessionStateManager()

    @override
    def submit(self, compiled_code: str) -> None:
        logger.debug("Executing Python model directly in SparkSession.")

        try:
            # Get clean execution context
            exec_globals = self._state_manager.get_clean_exec_globals(self._spark)

            # Execute the compiled Python model code
            exec(compiled_code, exec_globals)

        except Exception as e:
            logger.error(f"Python model execution failed: {e}")
            raise DbtRuntimeError(f"Python model execution failed: {e}") from e

        finally:
            # Clean up temp views to prevent state leakage
            try:
                self._state_manager.cleanup_temp_views(self._spark)
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup temp views: {cleanup_error}")
```

4. Add new `SessionPythonJobHelper`:
```python
class SessionPythonJobHelper(PythonJobHelper):
    """Helper for Python models executing directly in session mode."""

    tracker = PythonRunTracker()

    def __init__(self, parsed_model: dict, credentials: DatabricksCredentials) -> None:
        self.credentials = credentials
        self.parsed_model = ParsedPythonModel(**parsed_model)

        # Get SparkSession directly
        from pyspark.sql import SparkSession
        self._spark = SparkSession.builder.getOrCreate()

    def submit(self, compiled_code: str) -> None:
        submitter = SessionPythonSubmitter(self._spark)
        submitter.submit(compiled_code)
```

**File: [impl.py](dbt/adapters/databricks/impl.py)**

3. Register the new submission helper:
```python
@property
def python_submission_helpers(self) -> dict[str, type[PythonJobHelper]]:
    return {
        "job_cluster": JobClusterPythonJobHelper,
        "all_purpose_cluster": AllPurposeClusterPythonJobHelper,
        "serverless_cluster": ServerlessClusterPythonJobHelper,
        "workflow_job": WorkflowPythonJobHelper,
        "session": SessionPythonJobHelper,  # NEW
    }
```

4. Update `submit_python_job()` to auto-select session mode:
```python
def submit_python_job(self, parsed_model: dict, compiled_code: str) -> AdapterResponse:
    # Auto-select session submission when in session mode
    creds = self.config.credentials
    if creds.method == "session":
        if parsed_model["config"].get("submission_method") is None:
            parsed_model["config"]["submission_method"] = "session"

    # ... existing code ...
    return super().submit_python_job(parsed_model, compiled_code)
```

### Phase 5: Testing

**New Files:**
- `tests/unit/test_session.py` - Unit tests for session handle and cursor
- `tests/unit/test_session_python.py` - Unit tests for session Python submission
- `tests/functional/adapter/session/test_session_mixed_pipeline.py` - Integration tests

## Files to Modify

| File | Changes |
|------|---------|
| [credentials.py](dbt/adapters/databricks/credentials.py) | Add `method` field, auto-detection, validation |
| [connections.py](dbt/adapters/databricks/connections.py) | Add session dispatch in `open()`, capabilities caching |
| [impl.py](dbt/adapters/databricks/impl.py) | Register `SessionPythonJobHelper`, auto-select session mode |
| [python_submissions.py](dbt/adapters/databricks/python_models/python_submissions.py) | Add `SessionPythonSubmitter`, `SessionPythonJobHelper` |
| **NEW** [session.py](dbt/adapters/databricks/session.py) | `SessionCursorWrapper`, `DatabricksSessionHandle` |
| **NEW** [tests/unit/test_session.py](tests/unit/test_session.py) | Unit tests |

## Configuration

**profiles.yml for full session mode:**
```yaml
my_project:
  target: job_cluster
  outputs:
    job_cluster:
      type: databricks
      method: session
      catalog: main
      schema: my_schema
      # host/http_path/token NOT required
```

**Model-level override (optional):**
```sql
-- For Python models that need specific submission method
{{ config(submission_method='session') }}
```

**Environment variable:**
```bash
export DBT_DATABRICKS_SESSION_MODE=true
```

## Usage Scenarios

### Scenario 1: Run dbt from Databricks Notebook on Job Cluster
```python
# In a Databricks notebook task within a job
from dbt.cli.main import dbtRunner

dbt = dbtRunner()
result = dbt.invoke(["run"])  # Uses session mode automatically
```

### Scenario 2: Run dbt from Python Script Task
```python
# python_task.py - executed as a Python script task in a Databricks job
import subprocess
subprocess.run(["dbt", "run", "--profiles-dir", "/dbfs/path/to/profiles"])
```

### Scenario 3: Databricks Workflow with dbt Task
```yaml
# Databricks job definition
tasks:
  - task_key: run_dbt
    job_cluster_key: my_cluster
    python_wheel_task:
      package_name: my_dbt_project
      entry_point: run_dbt
    libraries:
      - pypi: {package: dbt-databricks}
```

## Verification Plan

1. **Unit tests**: `pytest tests/unit/test_session*.py`
2. **Local validation**: Test credentials validation for both modes
3. **Integration test**:
   ```python
   # Run in Databricks notebook on job cluster
   from dbt.cli.main import dbtRunner

   dbt = dbtRunner()

   # Test SQL model
   result = dbt.invoke(["run", "--select", "my_sql_model"])
   assert result.success

   # Test Python model
   result = dbt.invoke(["run", "--select", "my_python_model"])
   assert result.success

   # Test full pipeline
   result = dbt.invoke(["run"])
   assert result.success
   ```

## Session vs API Client Comparison

### Data Collection (Python Models)

**Both approaches write directly to tables without collecting to driver:**

1. **API Client (Jobs/Command API)**:
   - Executes: `df.write.saveAsTable("table_name")`
   - Runs in separate context/notebook
   - Data written directly to storage

2. **Session Mode (exec())**:
   - Executes: `df.write.saveAsTable("table_name")` (same code!)
   - Runs in same SparkSession
   - Data written directly to storage

**Result**: No difference in data collection behavior - both are distributed writes.

### Functional Differences

| Feature | API Client | Session Mode |
|---------|-----------|--------------|
| **Execution Location** | Separate job/context | Same SparkSession |
| **Isolation** | Isolated execution | Shared session state |
| **Retry Logic** | Built-in via Jobs API | Manual if needed |
| **Monitoring** | Databricks job UI | SparkUI only |
| **Permissions** | Requires cluster access permissions | Uses current session |
| **Async Execution** | Submits and polls | Blocks until complete |
| **Library Installation** | Via cluster config or job spec | Must be pre-installed |
| **Resource Limits** | Can specify per-job | Uses session limits |

### Session Mode vs API Client Trade-offs

**IMPORTANT CONTEXT**: These differences only apply to **Python model execution**. SQL models have never had these API client features. Session mode actually **improves** observability by allowing the entire dbt pipeline to run within a Databricks job.

| Aspect | SQL Models (Before) | Session Mode (SQL + Python) | API Client (Python only) |
|--------|---------------------|----------------------------|--------------------------|
| **Pipeline Observability** | ❌ No Databricks job tracking | ✅ **Full dbt run tracked in Databricks job** | ✅ Individual Python models tracked |
| **Cost** | Requires SQL Warehouse/All-Purpose | ✅ **Single job cluster** | Multiple job clusters |
| **Sequential Execution** | ✅ Follows DAG | ✅ **Follows DAG (intended)** | ✅ Follows DAG |
| **Per-Model Monitoring** | ❌ Not in Databricks UI | ❌ Not in Databricks UI | ✅ Each Python model visible |
| **Library Management** | Pre-installed | Pre-installed | ✅ Per-job dynamic install |
| **Failure Behavior** | Fails dbt run | Fails dbt run | ✅ Per-model retry |

**Key Insight**: The "drawbacks" listed are actually **not regressions** - SQL models never had per-model Databricks job monitoring, dynamic libraries, or isolated retry logic. Session mode brings:

✅ **New Capability**: Run entire dbt pipeline on job cluster (previously impossible)
✅ **Better Observability**: Whole dbt run visible in Databricks job (vs. no tracking before)
✅ **Cost Savings**: Single cluster for SQL + Python (vs. SQL Warehouse + separate job clusters)
✅ **Correct Behavior**: Sequential execution following data lineage (as intended)

**The Only Real Trade-off**: Per-Python-model granularity in Databricks UI vs. whole-pipeline execution efficiency.

### When to Use Each Approach

**Use Session Mode (Recommended for most use cases):**
- ✅ Running dbt from Databricks job (notebooks, tasks, workflows)
- ✅ Cost optimization is priority (single cluster for entire pipeline)
- ✅ Want Databricks job-level tracking of dbt runs
- ✅ Sequential execution following DAG is acceptable (it should be!)
- ✅ Libraries can be installed on cluster beforehand

**Use API Client (for specific advanced scenarios):**
- Need per-Python-model granular monitoring in Databricks UI
- Require per-model dynamic library installation
- Want independent retry logic for each Python model
- Need to run Python models on different cluster configurations
- Async submission of Python models (non-DAG execution)

## Limitations

**Actual Limitations:**
- **No API-based features**: Session mode doesn't have host/token, so API-dependent features (like standalone workflow job creation) won't work
- **No DBSQL query history**: Queries don't appear in Databricks SQL query history (but entire run appears in job logs)
- **Single cluster compute**: All models use same cluster resources (this is the goal for cost optimization)

**Not Limitations (these are correct behaviors):**
- ✅ Python models don't appear as separate jobs in Databricks UI → **Correct**: They're part of the main dbt job
- ✅ Models execute sequentially following DAG → **Correct**: This respects data dependencies
- ✅ Libraries must be pre-installed → **Normal**: Same as SQL models; install on cluster startup

## Design Decisions

1. **Configuration**: Use `method: session` in profiles.yml (aligns with dbt-spark)
2. **Python execution**: Direct `exec()` in current SparkSession (simplest, most compatible)
3. **Auto-detection**: When `DATABRICKS_RUNTIME_VERSION` is set and no host configured, default to session mode
4. **Backward compatibility**: Default behavior unchanged when `method` not specified with host/token present

## Migration Path

For existing users:
1. No changes required if using DBSQL mode (default)
2. To use session mode, add `method: session` to profile
3. Python models automatically use session submission when profile is in session mode

## FAQ

### Q: When will Python model results be collected to the driver?

**A: Never.** Both session mode and API client mode execute the same compiled code, which ends with:
```python
df.write.saveAsTable("table_name")
```

This writes the DataFrame directly to Delta/storage in a distributed manner. No `collect()` is called, so data stays distributed across the cluster. The only data transferred is metadata (schema, row counts, etc.).

### Q: What are the drawbacks of not using the API client for Python models?

**A: The "drawbacks" are actually not regressions - they're trade-offs that already existed for SQL models:**

**What you "lose" (but SQL models never had):**
1. Per-Python-model visibility in Databricks Jobs UI (vs. per-notebook jobs)
2. Per-Python-model retry logic (vs. whole dbt run retry)
3. Dynamic library installation per model

**What you GAIN with session mode:**
1. ✅ **Run entire pipeline on job cluster** (previously impossible)
2. ✅ **Databricks job tracking of full dbt run** (better than no tracking)
3. ✅ **70%+ cost savings** - Single cluster vs. SQL Warehouse + job clusters
4. ✅ **Faster execution** - No job submission overhead
5. ✅ **Correct DAG execution** - Sequential following lineage (as intended)
6. ✅ **Simpler setup** - No API authentication needed

**Context**: SQL models have always executed sequentially, blocked downstream models, and lacked per-model Databricks UI tracking. Session mode brings Python models to the same (correct) behavior while enabling the entire pipeline to run on cost-effective job clusters.

**Recommendation**: Use session mode for production pipelines running from Databricks jobs. It's more cost-effective, properly respects the DAG, and provides job-level observability.

### Q: How do I handle failures in session mode?

**A: Options:**

1. **Databricks job-level retry**: Configure the Databricks job that runs dbt to retry on failure
2. **dbt retry logic**: Use `dbt retry` command after failures
3. **Model-level error handling**: Add try/except in Python model code if needed
4. **Checkpointing**: Use incremental models to avoid re-running successful work

### Q: Can I mix session mode and API client mode?

**A: Not recommended.** Stick to one approach per dbt run for consistency. However, you could:
- Use session mode for dev/testing
- Use API client mode for production
- Configure via environment-specific profiles

## Future Considerations

- Consider submitting as PR to dbt-databricks upstream
- May need periodic sync with dbt-databricks updates if maintained as fork
- Could add support for hybrid mode (session SQL + Jobs API Python) if needed
- Consider adding session-level Python model parallelization using ThreadPoolExecutor
