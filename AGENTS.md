# AGENTS.md - AI Agent Guide for dbt-databricks

This guide helps AI agents quickly understand and work productively with the dbt-databricks adapter codebase.

## 🚀 Quick Start for Agents

### Project Overview

- **What**: dbt adapter for Databricks Lakehouse platform
- **Based on**: dbt-spark adapter with Databricks-specific enhancements
- **Key Features**: Unity Catalog support, Delta Lake, Python models, streaming tables
- **Language**: Python 3.9+ with Jinja2 SQL macros
- **Architecture**: Inherits from Spark adapter, extends with Databricks-specific functionality

### Essential Files to Understand

```
dbt/adapters/databricks/
├── impl.py                    # Main adapter implementation (DatabricksAdapter class)
├── connections.py             # Connection management and SQL execution
├── credentials.py             # Authentication (token, OAuth, Azure AD)
├── relation.py               # Databricks-specific relation handling
├── dbr_capabilities.py       # DBR version capability system
├── python_models/            # Python model execution on clusters
├── relation_configs/         # Table/view configuration management
└── catalogs/                 # Unity Catalog vs Hive Metastore logic

dbt/include/databricks/macros/ # Jinja2 SQL templates
├── adapters/                 # Core adapter macros
├── materializations/         # Model materialization strategies
├── relations/                # Table/view creation and management
└── utils/                    # Utility macros
```

## 🛠 Development Environment

**Prerequisites**: Python 3.9+ installed on your system

**Install Hatch** (recommended):

For Linux:

```bash
# Download and install standalone binary
curl -Lo hatch.tar.gz https://github.com/pypa/hatch/releases/latest/download/hatch-x86_64-unknown-linux-gnu.tar.gz
tar -xzf hatch.tar.gz
mkdir -p $HOME/bin
mv hatch $HOME/bin/hatch
chmod +x $HOME/bin/hatch
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.zshrc
export PATH="$HOME/bin:$PATH"

# Create default environment (Hatch installs needed Python versions)
hatch env create
```

For other platforms: see https://hatch.pypa.io/latest/install/

**Essential commands**:

```bash
hatch run code-quality           # Format, lint, type-check
hatch run unit                   # Run unit tests
hatch run cluster-e2e            # Run functional tests

# For specific tests, use pytest directly:
hatch run pytest path/to/test_file.py::TestClass::test_method -v
```

> 📖 **See [Development Guide](docs/dbt-databricks-dev.md)** for comprehensive setup documentation
> 📖 **See [Testing Guide](docs/testing.md)** for comprehensive testing documentation

## 🧪 Testing Strategy

### Test Types & When to Use

1. **Unit Tests** (`tests/unit/`): Fast, isolated, no external dependencies

   - Test individual functions, utility methods, SQL generation
   - Mock external dependencies (database calls, API calls)
   - Run with: `hatch run unit`

2. **Functional Tests** (`tests/functional/`): End-to-end with real Databricks
   - Test complete dbt workflows (run, seed, test, snapshot)
   - Require live Databricks workspace
   - Run with: `hatch run cluster-e2e` (or `uc-cluster-e2e`, `sqlw-e2e`)

### Test Environments

- **HMS Cluster** (`databricks_cluster`): Legacy Hive Metastore
- **Unity Catalog Cluster** (`databricks_uc_cluster`): Modern UC features
- **SQL Warehouse** (`databricks_uc_sql_endpoint`): Serverless compute

### Writing Tests

#### Unit Test Example

```python
from dbt.adapters.databricks.utils import redact_credentials

def test_redact_credentials():
    sql = "WITH (credential ('KEY' = 'SECRET_VALUE'))"
    expected = "WITH (credential ('KEY' = '[REDACTED]'))"
    assert redact_credentials(sql) == expected
```

#### Macro Test Example

```python
from tests.unit.macros.base import MacroTestBase

class TestCreateTable(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"  # File in macros/relations/table/

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/table"]

    def test_create_table_sql(self, template_bundle):
        result = self.run_macro(template_bundle.template, "create_table",
                               template_bundle.relation, "select 1")
        expected = "create table `database`.`schema`.`table` as (select 1)"
        self.assert_sql_equal(result, expected)
```

#### Functional Test Example

```python
from dbt.tests import util

class TestIncrementalModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": """
                {{ config(materialized='incremental', unique_key='id') }}
                select 1 as id, 'test' as name
            """
        }

    def test_incremental_run(self, project):
        results = util.run_dbt(["run"])
        assert len(results) == 1
        # Verify table exists and has expected data
        results = project.run_sql("select count(*) from my_model", fetch="all")
        assert results[0][0] == 1
```

## 🏗 Architecture Deep Dive

### Adapter Inheritance Chain

```
DatabricksAdapter (impl.py)
  ↳ SparkAdapter (from dbt-spark)
    ↳ SQLAdapter (from dbt-core)
      ↳ BaseAdapter (from dbt-core)
```

### Key Components

#### DBR Capability System (`dbr_capabilities.py`)

- **Purpose**: Centralized management of DBR version-dependent features
- **Key Features**:
  - Per-compute caching (different clusters can have different capabilities)
  - Named capabilities instead of magic version numbers
  - Automatic detection of DBR version and SQL warehouse environments
- **Supported Capabilities**:
  - `TIMESTAMPDIFF` (DBR 10.4+): Advanced date/time functions
  - `INSERT_BY_NAME` (DBR 12.2+): Name-based column matching in INSERT
  - `ICEBERG` (DBR 14.3+): Apache Iceberg table format
  - `COMMENT_ON_COLUMN` (DBR 16.1+): Modern column comment syntax
  - `JSON_COLUMN_METADATA` (DBR 16.2+): Efficient metadata retrieval
- **Usage in Code**:
  ```python
  # In Python code
  if adapter.has_capability(DBRCapability.ICEBERG):
      # Use Iceberg features

  # In Jinja macros
  {% if adapter.has_dbr_capability('comment_on_column') %}
      COMMENT ON COLUMN ...
  {% else %}
      ALTER TABLE ... ALTER COLUMN ...
  {% endif %}

  {% if adapter.has_dbr_capability('insert_by_name') %}
      INSERT INTO table BY NAME SELECT ...
  {% else %}
      INSERT INTO table SELECT ... -- positional
  {% endif %}
  ```
- **Adding New Capabilities**:
  1. Add to `DBRCapability` enum
  2. Add `CapabilitySpec` with version requirements
  3. Use `has_capability()` or `require_capability()` in code
- **Important**: Each compute resource (identified by `http_path`) maintains its own capability cache

#### Connection Management (`connections.py`)

- Extends Spark connection manager for Databricks
- Manages connection lifecycle and query execution
- Handles query comments and context tracking
- Integrates with `credentials.py` for authentication and `handle.py` for cursor operations

#### Authentication & Credentials (`credentials.py`)

- Defines credential dataclass with all auth methods (token, OAuth, Azure AD)
- Handles credential validation and session properties
- Manages compute resource configuration

#### SQL Execution (`handle.py`)

- Provides cursor wrapper for Databricks SQL connector
- Implements retry logic and connection pooling
- Handles SQL execution details and error handling

#### Relation Handling (`relation.py`)

- Extends Spark relations with Databricks features
- Handles Unity Catalog 3-level namespace (catalog.schema.table)
- Manages relation metadata and configuration

#### Python Models (`python_models/`)

- Executes Python models on Databricks clusters
- Supports multiple submission methods (jobs, workflows, serverless)
- Handles dependency management and result collection

#### Macros (`dbt/include/databricks/macros/`)

- Jinja2 templates that generate SQL
- Override Spark macros with Databricks-specific logic
- Handle materializations (table, view, incremental, snapshot)
- Implement Databricks features (liquid clustering, column masks, tags)
- **Important**: To override a `spark__macro_name` macro, create `databricks__macro_name` (NOT `spark__macro_name`)

### Configuration System

Models can be configured with Databricks-specific options:

```sql
{{ config(
    materialized='table',
    file_format='delta',
    liquid_clustering=['column1', 'column2'],
    tblproperties={'key': 'value'},
    column_tags={'pii_col': ['sensitive']},
    location_root='/mnt/external/'
) }}
```

## 🔧 Common Development Tasks

### Adding New Materialization

1. Create macro in `macros/materializations/`
2. Implement SQL generation logic
3. Add configuration options to relation configs
4. Write unit tests for macro
5. Write functional tests for end-to-end behavior
6. Update documentation

### Adding New Adapter Method

1. Add method to `DatabricksAdapter` class in `impl.py`
2. Implement database interaction logic
3. Add corresponding macro if SQL generation needed
4. Write unit tests with mocked database calls
5. Write functional tests with real database

### Modifying SQL Generation

1. Locate relevant macro in `macros/` directory
2. Test current behavior with unit tests
3. Modify macro logic
4. Update unit tests to verify new behavior
5. Run affected functional tests to ensure no regressions

### Adding Configuration Option

1. Add field to appropriate config class in `relation_configs/`
2. Update macro to use new configuration
3. Add validation logic if needed
4. Write tests for both valid and invalid configurations

## 🐛 Debugging Guide

### Common Issues

1. **SQL Generation**: Use macro unit tests with `assert_sql_equal()`
2. **Connection Problems**: Check credentials and environment variables
3. **Python Model Failures**: Check cluster configuration and dependencies
4. **Test Failures**: Review logs in `logs/` directory, look for red text

### Debugging Tools

- **IDE Test Runner**: Set breakpoints and step through code
- **Log Analysis**: dbt generates detailed debug logs by default
- **SQL Inspection**: Print generated SQL in macros for debugging
- **Mock Inspection**: Verify mocked calls in unit tests

## 📚 Key Resources

### Documentation

- **Development**: `docs/dbt-databricks-dev.md` - Setup and workflow
- **Testing**: `docs/testing.md` - Comprehensive testing guide
- **DBR Capabilities**: `docs/dbr-capability-system.md` - Version-dependent features
- **Contributing**: `CONTRIBUTING.MD` - Code standards and PR process
- **User Docs**: [docs.getdbt.com](https://docs.getdbt.com/reference/resource-configs/databricks-configs)

### Important Files for Agents

- `pyproject.toml` - Project configuration, dependencies, tool settings
- `test.env.example` - Template for test environment variables
- `tests/conftest.py` - Global test configuration
- `tests/profiles.py` - Test database profiles

### Code Patterns to Follow

1. **Error Handling**: Use dbt's exception classes, provide helpful messages
2. **Logging**: Use `logger` from `dbt.adapters.databricks.logging`
3. **SQL Generation**: Prefer macros over Python string manipulation
4. **Testing**: Write both unit and functional tests for new features
5. **Configuration**: Use dataclasses with validation for new config options
6. **Imports**: Always import at the top of the file, never use local imports within functions or methods
7. **Version Checks**: Use capability system instead of direct version comparisons:
   - ❌ `if adapter.compare_dbr_version(16, 1) >= 0:`
   - ✅ `if adapter.has_capability(DBRCapability.COMMENT_ON_COLUMN):`
   - ✅ `{% if adapter.has_dbr_capability('comment_on_column') %}`

## 🚨 Common Pitfalls for Agents

1. **Don't modify dbt-spark behavior** without understanding inheritance
2. **Always run code-quality** before committing changes
3. **Test on multiple environments** (HMS, UC cluster, SQL warehouse)
4. **Mock external dependencies** in unit tests properly
5. **Use appropriate test fixtures** from dbt-tests-adapter
6. **Follow SQL normalization** in test assertions with `assert_sql_equal()`
7. **Handle Unity Catalog vs HMS differences** in feature implementations
8. **Consider backward compatibility** when modifying existing behavior
9. **Use capability system for version checks** - Never add new `compare_dbr_version()` calls
10. **Remember per-compute caching** - Different clusters may have different capabilities in the same run

## 🎯 Success Metrics

When working on this codebase, ensure:

- [ ] All tests pass (`hatch run code-quality && hatch run unit`)
- [ ] **CRITICAL: Run affected functional tests before declaring success**
  - If you modified connection/capability logic: Run tests that use multiple computes or check capabilities
  - If you modified incremental materializations: Run `tests/functional/adapter/incremental/`
  - If you modified Python models: Run `tests/functional/adapter/python_model/`
  - If you modified macros: Run tests that use those macros
  - **NEVER declare "mission accomplished" without running functional tests for affected features**
- [ ] New features have both unit and functional tests
- [ ] SQL generation follows Databricks best practices
- [ ] Changes maintain backward compatibility
- [ ] Code follows project style guidelines

---

_This guide is maintained by the dbt-databricks team. When making significant architectural changes, update this guide to help future agents understand the codebase._
