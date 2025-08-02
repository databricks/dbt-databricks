# Testing dbt-databricks

This guide covers the comprehensive testing strategy and practices for the dbt-databricks adapter.

## Overview

The dbt-databricks adapter uses a multi-layered testing approach to ensure reliability and compatibility across different Databricks environments:

- **Unit Tests**: Fast, isolated tests for individual components
- **Functional Tests**: End-to-end integration tests with real Databricks clusters

## Test Structure

### Directory Organization

```
tests/
├── unit/                     # Unit tests (no external dependencies)
│   ├── api_client/           # API client functionality
│   ├── relation_configs/     # Relation configuration logic
│   ├── python/               # Python model handling
│   ├── macros/               # Macro testing framework
│   └── test_adapter.py       # Core adapter functionality
├── functional/               # Integration tests (requires Databricks)
│   └── adapter/              # Adapter-specific functionality
│       ├── basic/            # Basic model operations
│       ├── incremental/      # Incremental materialization
│       ├── python_model/     # Python model execution
│       ├── streaming_tables/ # Streaming table features
│       └── ...               # Feature-specific test suites
├── conftest.py               # Global pytest configuration, including the default compute used when running tests in your IDE
└── profiles.py               # Test profile configurations
```

## Test Environments

### Databricks Environments

The adapter supports testing against three different Databricks compute environments:

1. **HMS Cluster** (`databricks_cluster`)

   - Traditional Hive Metastore setup
   - Some features not supported on this legacy environment

2. **Unity Catalog Cluster** (`databricks_uc_cluster`)

   - Unity Catalog-enabled cluster
   - Full feature support for modern Databricks capabilities

3. **SQL Warehouse** (`databricks_uc_sql_endpoint`)
   - Serverless SQL compute
   - Occasionally has feature limitations for non-SQL operations

### Environment Configuration

Test environments are configured through environment variables:

```bash
# Required for all environments
export DBT_DATABRICKS_HOST_NAME="your-workspace.databricks.com"

# Authentication (choose one method)
# Option 1: Personal Access Token
export DBT_DATABRICKS_TOKEN="your-token"

# Option 2: Databricks OAuth
export DBT_DATABRICKS_CLIENT_ID="your-client-id"
export DBT_DATABRICKS_CLIENT_SECRET="your-client-secret"

# Option 3: Azure AD OAuth
export DBT_DATABRICKS_AZURE_CLIENT_ID="your-azure-client-id"
export DBT_DATABRICKS_AZURE_CLIENT_SECRET="your-azure-client-secret"

# Compute-specific paths
export DBT_DATABRICKS_CLUSTER_HTTP_PATH="/sql/protocolv1/o/.../..."
export DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH="/sql/protocolv1/o/.../..."
export DBT_DATABRICKS_UC_ENDPOINT_HTTP_PATH="/sql/warehouses/..."

# Unity Catalog settings
export DBT_DATABRICKS_UC_INITIAL_CATALOG="main"
export DBT_DATABRICKS_UC_INITIAL_SCHEMA="default_schema"
```

### Getting Test Environment Access

- **External contributors**: You'll need access to a Databricks workspace with the appropriate cluster types. See the [dbt-databricks documentation](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup) for workspace setup guidance.

- **Databricks employees**: Contact @benc-db for access to the standard test environment configuration values.

## Running Tests

### Using Hatch (Recommended)

Hatch manages Python environments and dependencies automatically:

```bash
# Unit tests only (fast)
hatch run unit

# Functional tests by environment
hatch run cluster-e2e           # HMS cluster
hatch run uc-cluster-e2e        # Unity Catalog cluster
hatch run sqlw-e2e              # SQL Warehouse

# Unit tests across Python versions (for compatibility)
hatch run test:unit             # All Python versions (3.9-3.12)
```

### Advanced Test Options

For more advanced testing scenarios:

- **Pytest documentation**: See the [pytest documentation](https://docs.pytest.org/) for comprehensive test selection, filtering, and debugging options
- **Hatch integration**: See [Hatch's documentation on passing arguments to scripts](https://hatch.pypa.io/latest/config/environment/overview/#scripts) for how to pass additional arguments to test commands

## Writing Tests

### Unit Tests

Unit tests should be fast, isolated, and not require external dependencies.

#### Example: Testing Utility Functions

```python
from dbt.adapters.databricks.utils import redact_credentials, quote

class TestDatabricksUtils:
    def test_redact_credentials_removes_sensitive_data(self):
        """Test that credentials are properly redacted in SQL strings"""
        sql = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY' = 'SECRET_VALUE')\n"
            "  )\n"
            "fileformat = parquet"
        )
        expected = (
            "copy into target_table\n"
            "from source_table\n"
            "  WITH (\n"
            "    credential ('KEY' = '[REDACTED]')\n"
            "  )\n"
            "fileformat = parquet"
        )

        assert redact_credentials(sql) == expected

    def test_quote_adds_backticks(self):
        """Test that identifiers are properly quoted with backticks"""
        assert quote("table_name") == "`table_name`"
```

#### Macro Tests

Macro tests verify Jinja2 macro functionality by rendering templates and comparing SQL output. The `MacroTestBase` class provides a comprehensive testing framework.

##### Basic Setup

```python
from unittest.mock import Mock
import pytest
from tests.unit.macros.base import MacroTestBase

class TestCreateViewMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        """Required: Name of the template file to test"""
        return "create.sql"  # File in dbt/include/databricks/macros/

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        """Required: Macro directories to load relative to dbt/include/databricks/"""
        return ["macros", "macros/relations/view"]
```

##### Required Fixtures

- **`template_name`**: Template file to test (e.g., `"adapters.sql"`, `"create.sql"`)
- **`macro_folders_to_load`**: List of macro directories to load

##### Optional Configuration Fixtures

```python
@pytest.fixture(scope="class")
def databricks_template_names(self) -> list:
    """Load additional Databricks templates for macro dependencies"""
    return ["adapters.sql", "relations/table.sql"]

@pytest.fixture(scope="class")
def spark_template_names(self) -> list:
    """Load Spark templates when inheriting from dbt-spark macros"""
    return ["adapters.sql"]
```

##### Context Manipulation

Many macros depend on dbt configuration, variables, or adapter state. You need to manipulate the test context to simulate different runtime conditions.

**Config Manipulation**: Use this when testing macros that read model configuration or adapter settings. For example, table creation macros often check `file_format`, `tblproperties`, or `liquid_clustering` settings to generate appropriate SQL.

```python
def test_macro_with_config(self, config, template_bundle):
    """Test how macros respond to different model configurations"""
    config["tblproperties"] = {"key": "value"}        # Model-level table properties
    config["file_format"] = "delta"                   # Storage format setting
    config["liquid_clustering"] = ["col1", "col2"]    # Clustering configuration

    result = self.run_macro(template_bundle.template, "create_table_as",
                           template_bundle.relation, "select 1")
    # Verify the generated SQL includes the configuration
    assert "tblproperties" in result
    assert "delta" in result.lower()
```

**Variable Manipulation**: Use this when testing macros that use `var()` to read dbt project variables. In dbt-databricks, this is primarily used for feature flags that allow users to skip certain operations.

```python
def test_macro_with_variables(self, var, template_bundle):
    """Test macros that use dbt variables for feature flags"""
    # Test the DATABRICKS_SKIP_OPTIMIZE variable (real example from optimize.sql)
    var["DATABRICKS_SKIP_OPTIMIZE"] = True

    result = self.run_macro(template_bundle.template, "optimize", template_bundle.relation)
    # When skip flag is set, macro should return empty string
    assert result == ""

def test_macro_ignores_extraneous_variables(self, var, template_bundle):
    """Test that macros handle unexpected variables gracefully"""
    var["SOME_RANDOM_VAR"] = True

    result = self.run_macro(template_bundle.template, "my_macro")
    # Macro should work normally despite unexpected variables
    assert result  # Should still produce output
```

**Context Mocking**: Use this when testing macros that call other dbt functions or adapter methods. This is essential for isolating the macro under test from external dependencies.

```python
def test_macro_with_context_mocks(self, template_bundle):
    """Mock external dependencies to isolate macro behavior"""
    # Mock dbt built-in functions that the macro calls
    template_bundle.context["get_columns_in_query"] = Mock(return_value=[])
    template_bundle.context["column_mask_exists"] = Mock(return_value=False)

    # Mock adapter methods to simulate database state
    template_bundle.context["adapter"].get_relation = Mock(return_value=None)
    template_bundle.context["adapter"].run_query = Mock(return_value=Mock(table=[]))

    result = self.run_macro(template_bundle.template, "my_macro",
                           template_bundle.relation)
    # Test that the macro handles the mocked responses correctly
```

##### Custom Relations and Fixtures

The default `relation` fixture provides a basic table relation, but many macros need to test different relation types or specific database configurations. Custom relation fixtures let you simulate various database scenarios.

**Schema Relations**: Use when testing macros that operate on schemas rather than tables (e.g., `show_tables_sql`, catalog operations).

```python
@pytest.fixture
def mock_schema_relation(self):
    """Schema-level relation for testing metadata operations"""
    relation = Mock()
    relation.database = "test_db"
    relation.schema = "test_schema"
    relation.render = Mock(return_value="`test_db`.`test_schema`")
    return relation
```

**Feature-Specific Relations**: Create relations that simulate specific Databricks features like external locations, Unity Catalog, or streaming tables.

```python
@pytest.fixture
def external_location_relation(self):
    """External location relation for testing COPY INTO and external table macros"""
    relation = Mock()
    relation.database = "catalog"
    relation.schema = "schema"
    relation.identifier = "table"
    relation.render = Mock(return_value="`catalog`.`schema`.`table`")
    relation.is_external_location = True
    return relation

@pytest.fixture
def streaming_table_relation(self):
    """Streaming table relation for testing DLT-specific macros"""
    relation = Mock()
    relation.type = "streaming_table"
    relation.is_streaming_table = True
    return relation
```

##### Running Macros

Different macro execution methods serve different testing needs. Choose the right method based on what you're testing and how the macro is structured.

```python
def test_macro_execution_patterns(self, template_bundle):
    # Method 1: Basic macro execution with arguments
    # Use for simple macros that take explicit arguments
    result = self.run_macro(template_bundle.template, "macro_name", "arg1", "arg2")

    # Method 2: Relation-first pattern (very common in dbt-databricks)
    # Use for macros that follow the pattern macro(relation, other_args...)
    # Automatically passes the test relation as the first argument
    result = self.render_bundle(template_bundle, "create_table_as", "select 1")

    # Method 3: Raw execution (preserves whitespace/formatting)
    # Use when you need to test exact formatting, spacing, or newlines
    # Most tests should use run_macro() instead, which normalizes whitespace
    raw_result = self.run_macro_raw(template_bundle.template, "macro_name", "arg1")

    # Method 4: Custom helper method for complex macros
    # Use for macros with many arguments or complex setup
    # Makes tests more readable and reduces duplication
    result = self.render_create_view_as(template_bundle, sql="select 1")

def render_create_view_as(self, template_bundle, sql="select 1"):
    """Helper method reduces complexity in individual test methods"""
    return self.run_macro(
        template_bundle.template,
        "databricks__create_view_as",  # Full macro name with adapter prefix
        template_bundle.relation,      # Relation as first argument
        sql                            # SQL to wrap in view
    )
```

##### SQL Comparison and Assertions

SQL generated by macros often contains varying whitespace, line breaks, and case differences. Use the appropriate assertion method based on how strict your comparison needs to be.

```python
def test_sql_comparison_methods(self, template_bundle):
    result = self.run_macro(template_bundle.template, "create_table",
                           template_bundle.relation)

    # Method 1: Exact string comparison (after normalization) - PREFERRED
    # Use for most tests - handles whitespace, case, and formatting differences
    # Provides clear error messages showing exactly what differs
    expected = "create table `some_database`.`some_schema`.`some_table` as (select 1)"
    self.assert_sql_equal(result, expected)

    # Method 2: Manual normalization for debugging
    # Use when assert_sql_equal fails and you need to see the normalized versions
    # Helpful for understanding why a test is failing
    clean_result = self.clean_sql(result)
    clean_expected = self.clean_sql(expected)
    assert clean_result == clean_expected, f"Expected: {clean_expected}, Got: {clean_result}"

    # Method 3: Partial matching for complex queries
    # Use when testing specific SQL elements without caring about exact formatting
    # Good for testing that required elements are present
    assert "`some_database`.`some_schema`.`some_table`" in result
    assert "create table" in result.lower()

    # Method 4: Complex validation
    # Use for testing multiple specific requirements
    lines = result.split('\n')
    assert any('CREATE TABLE' in line.upper() for line in lines)
    assert any('TBLPROPERTIES' in line.upper() for line in lines)
```

##### Advanced Testing Patterns

Complex macros require more sophisticated testing approaches. These patterns help test macros that interact with external systems, have conditional logic, or depend on multiple other macros.

```python
class TestAdvancedMacroPatterns(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters.sql"

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        # Load dependencies when testing complex macros that call other macros
        # Essential for materialization macros that use table/view creation macros
        return ["relations/table.sql", "materializations/table.sql"]

    @pytest.fixture(autouse=True, scope="function")
    def setup_mocks(self, context):
        """Auto-setup common mocks for all tests in this class

        Use autouse fixtures when all tests in a class need the same mocks.
        Reduces repetition and ensures consistent test setup.
        """
        context["adapter"].get_columns_in_relation = Mock(return_value=[])
        context["load_result"] = Mock(return_value={"data": []})

    def test_macro_with_database_operations(self, template_bundle):
        """Test macros that make adapter calls during rendering

        Some macros call adapter.run_query() to get metadata or check table state.
        Mock these calls to test the macro logic without database dependencies.
        """
        # Mock adapter calls that the macro will make
        template_bundle.context["adapter"].run_query = Mock(
            return_value=Mock(table=[["result1"], ["result2"]])
        )

        result = self.run_macro(template_bundle.template, "get_table_metadata",
                               template_bundle.relation)

        # Verify both the SQL generated and adapter calls made
        expected_sql = "describe table extended `some_database`.`some_schema`.`some_table`"
        self.assert_sql_equal(result, expected_sql)

        # Verify adapter was called correctly (important for side-effect testing)
        template_bundle.context["adapter"].run_query.assert_called_once()

    def test_conditional_macro_logic(self, config, template_bundle):
        """Test macros with conditional rendering based on config

        Many Databricks macros have optional features (optimization, clustering, etc.)
        Test both enabled and disabled states to ensure correct behavior.
        """
        # Test with feature enabled
        config["enable_feature"] = True
        result_enabled = self.run_macro(template_bundle.template, "conditional_macro")
        assert "feature_sql" in result_enabled

        # Test with feature disabled - macro should handle gracefully
        config["enable_feature"] = False
        result_disabled = self.run_macro(template_bundle.template, "conditional_macro")
        assert "feature_sql" not in result_disabled

    def test_error_handling_macro(self, template_bundle):
        """Test how macros handle invalid inputs or missing dependencies

        Good macros fail gracefully with helpful error messages.
        Test edge cases to ensure robust behavior.
        """
        # Test with missing required config
        with pytest.raises(Exception) as exc_info:
            self.run_macro(template_bundle.template, "macro_requiring_config")
        assert "Missing required config" in str(exc_info.value)
```

##### Best Practices

1. **Use class-scoped fixtures** for template setup to avoid reloading
2. **Create custom relation fixtures** for specific test scenarios
3. **Use `assert_sql_equal()`** for SQL comparisons (handles normalization)
4. **Mock adapter calls** that your macros make to external systems
5. **Test conditional logic** by varying config and context values
6. **Use helper methods** for complex macro invocation patterns
7. **Test error conditions** by setting up invalid contexts/configs
8. **Separate SQL generation from execution** when facing complex mocking scenarios

##### Macro Design for Testability

When you have a macro that both generates SQL and executes it (requiring complex adapter mocking), consider splitting it into two macros:

```sql
{# Pure SQL generation macro - easy to test #}
{% macro get_table_metadata_sql(relation) %}
  DESCRIBE TABLE EXTENDED {{ relation }}
{% endmacro %}

{# Execution macro - calls the SQL generator #}
{% macro get_table_metadata(relation) %}
  {% set sql = get_table_metadata_sql(relation) %}
  {% set result = run_query(sql) %}
  {{ return(result.table) }}
{% endmacro %}
```

**Benefits:**

- The SQL generation macro (`get_table_metadata_sql`) can be tested with simple string assertions
- The execution macro (`get_table_metadata`) has minimal logic to test
- Easier to debug SQL generation issues separately from execution issues
- Users can call either macro depending on whether they want SQL or results

**Testing approach:**

```python
def test_metadata_sql_generation(self, template_bundle):
    """Test SQL generation without mocking adapter calls"""
    result = self.run_macro(template_bundle.template, "get_table_metadata_sql",
                           template_bundle.relation)
    expected = "describe table extended `some_database`.`some_schema`.`some_table`"
    self.assert_sql_equal(result, expected)

def test_metadata_execution(self, template_bundle):
    """Test execution logic with minimal mocking"""
    template_bundle.context["run_query"] = Mock(
        return_value=Mock(table=[["col1", "string"], ["col2", "int"]])
    )
    result = self.run_macro(template_bundle.template, "get_table_metadata",
                           template_bundle.relation)
    # Test that it returns the table results correctly
    assert len(result) == 2
```

##### Common Patterns

```python
# Pattern 1: Testing materialization macros
def test_materialization_macro(self, config, template_bundle):
    config["materialized"] = "table"
    config["file_format"] = "delta"
    result = self.render_bundle(template_bundle, "materialization_macro")

# Pattern 2: Testing adapter dispatch
def test_adapter_specific_macro(self, template_bundle):
    result = self.run_macro(template_bundle.template, "databricks__specific_macro")

# Pattern 3: Testing macros with complex context
def test_incremental_macro(self, template_bundle):
    template_bundle.context["is_incremental"] = Mock(return_value=True)
    template_bundle.context["this"] = template_bundle.relation
    result = self.run_macro(template_bundle.template, "incremental_strategy")
```

### Functional Tests

Functional tests require a live Databricks environment and test end-to-end functionality. These tests work by creating a temporary mini-dbt project in an isolated schema, running dbt operations with `run_dbt()`, and verifying the results.

#### How Functional Tests Work

Functional tests create a complete dbt project environment for each test:

1. **Project Setup**: Test fixtures create models, seeds, and configuration files
2. **Execution**: Tests run dbt commands (`dbt run`, `dbt seed`, etc.) via `run_dbt()`
3. **Verification**: Tests query the database to verify expected results
4. **Cleanup**: Framework automatically cleans up the test schema

**Common Testing Pattern:**
A typical functional test uses seeds to define expected final state, runs the dbt project, then compares actual results to the seeded expected data:

```python
class TestIncrementalModel:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_result.csv": """id,name,status
1,Alice,active
2,Bob,inactive
3,Charlie,active"""
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": """
                {{ config(materialized='incremental', unique_key='id') }}
                select 1 as id, 'Alice' as name, 'active' as status
                union all
                select 2 as id, 'Bob' as name, 'inactive' as status
                union all
                select 3 as id, 'Charlie' as name, 'active' as status
            """
        }

    def test_incremental_behavior(self, project):
        # Seed the expected results
        util.run_dbt(["seed"])

        # Run the incremental model
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Compare actual vs expected results
        util.check_relations_equal(project.adapter,
                                 ["incremental_model", "expected_result"])
```

**Evaluating Metadata with Arbitrary SQL:**
For tests that need to verify table properties, constraints, or other metadata, you can execute arbitrary SQL using `project.run_sql()`:

```python
class TestTableMetadata:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": """
                {{ config(
                    materialized='table',
                    tblproperties={'my_property': 'my_value'}
                ) }}
                select 1 as id, 'test' as name
            """
        }

    def test_table_properties_are_set(self, project):
        # Run the model
        util.run_dbt(["run"])

        # Query table properties to verify they were set correctly
        results = project.run_sql(
            f"""
            SHOW TBLPROPERTIES {project.database}.{project.test_schema}.my_table
            """,
            fetch="all"
        )

        # Verify the property was set
        property_values = {row[0]: row[1] for row in results}
        assert property_values.get('my_property') == 'my_value'

    def test_table_constraints(self, project):
        # Create a table with constraints
        project.run_sql("""
            CREATE TABLE test_constraints (
                id INT NOT NULL,
                name STRING
            ) USING DELTA
        """)

        # Query information schema to verify constraints
        constraints = project.run_sql(
            f"""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = '{project.test_schema}'
            AND table_name = 'test_constraints'
            """,
            fetch="all"
        )

        assert len(constraints) > 0
        assert any('NOT NULL' in str(constraint) for constraint in constraints)
```

**Fixture Organization Pattern:**
Following dbt Labs convention, SQL and CSV content is typically separated into dedicated fixture files rather than embedded in test files:

```python
# tests/functional/adapter/incremental/fixtures.py
incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, 'Alice' as name, 'active' as status
union all
select 2 as id, 'Bob' as name, 'inactive' as status
union all
select 3 as id, 'Charlie' as name, 'active' as status
"""

expected_result_csv = """id,name,status
1,Alice,active
2,Bob,inactive
3,Charlie,active"""
```

```python
# tests/functional/adapter/incremental/test_incremental.py
from tests.functional.adapter.incremental import fixtures

class TestIncrementalModel:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_result.csv": fixtures.expected_result_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_model.sql": fixtures.incremental_model_sql}

    def test_incremental_behavior(self, project):
        # Test implementation same as above
```

This separation improves readability and allows fixture reuse across multiple test files.

#### dbt Labs Test Inheritance

Most functional tests inherit from standardized test classes in the dbt test adapter library. These base classes define common test scenarios that all adapters should support, ensuring consistency across the dbt ecosystem.

```python
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedConfigFullRefreshOff,
    BaseSeedCustomSchema,
    BaseSimpleSeedEnabledViaConfig,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin

class TestDatabricksSeeds(MaterializationV2Mixin, BaseSeedConfigFullRefreshOff):
    """Inherit standard seed tests with Databricks-specific setup"""
    pass

class TestDatabricksCustomSchema(BaseSeedCustomSchema):
    """Test custom schema functionality - may need adapter-specific modifications"""

    def test_seed_with_custom_schema(self, project):
        # Override base test method if Databricks behavior differs
        results = util.run_dbt(["seed"])
        assert len(results) == 1
        # Add Databricks-specific assertions here
```

**Why inherit from dbt Labs tests:**

- Ensures your adapter meets dbt execution engine requirements
- Provides comprehensive test coverage for standard dbt features
- Automatically tests edge cases that adapter developers might miss
- Maintains compatibility as dbt Core evolves

**When to modify inherited tests:**

- Databricks has different SQL syntax requirements
- Features behave differently on Databricks (e.g., CASCADE operations not supported)
- Additional Databricks-specific functionality needs verification
- Different error messages or response formats

#### dbt Test Framework Fixtures

The dbt adapter test framework provides several key fixtures for setting up test data and configurations. Understanding these fixtures is essential for writing effective functional tests.

**Key fixtures:**

- `models`: Dictionary of model files (SQL) to create in the test project
- `seeds`: Dictionary of seed files (CSV data) to load
- `macros`: Custom macros for the test project
- `project_config_update`: Updates to the dbt project configuration
- `project`: The main test project instance for running dbt commands

**Finding fixture definitions:**

- Base fixture definitions are in the `dbt-tests-adapter` package
- Look in `dbt.tests.fixtures.project` for core fixtures
- Many fixtures are defined in the specific test base classes you inherit from
- Check the `conftest.py` files in dbt Core and dbt-tests-adapter repositories

**Automatic cleanup:**
Objects created through the test framework (models, seeds, etc.) in the test's primary schema are automatically cleaned up after each test. You only need manual cleanup for:

- Objects created outside the test schema
- External resources (volumes, external locations)
- Objects created through direct SQL execution rather than dbt commands

```python
class TestWithAutomaticCleanup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id"  # Automatically cleaned up
        }

    def test_model_creation(self, project):
        results = util.run_dbt(["run"])
        # No cleanup needed - framework handles it

class TestWithManualCleanup:
    def test_external_object_creation(self, project):
        # Create object outside test schema - needs manual cleanup
        project.run_sql("CREATE SCHEMA IF NOT EXISTS external_schema")

        try:
            # Test logic here
            pass
        finally:
            # Manual cleanup for external objects
            project.run_sql("DROP SCHEMA IF EXISTS external_schema")
```

#### Custom Functional Tests

For Databricks-specific features, write custom functional tests:

```python
import pytest
from dbt.tests import util

class TestDatabricksSpecificFeature:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id"
        }

    def test_feature_works(self, project):
        # Run dbt command
        results = util.run_dbt(["run"])
        assert len(results) == 1

        # Verify results in database
        results = project.run_sql("select * from my_model", fetch="all")
        assert len(results) == 1
```

#### Environment-Specific Tests

Use profile skipping for environment-specific features:

```python
# Only run on Unity Catalog environments
@pytest.mark.skip_profile("databricks_cluster")
class TestUnityCatalogFeature:
    # Test implementation
```

## Test Data Management

### Cleanup

Tests should clean up after themselves:

```python
class TestWithCleanup:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        # Setup
        yield
        # Cleanup
        project.run_sql("DROP TABLE IF EXISTS my_test_table")
```

### Temporary Objects

Use the project's test schema for temporary objects:

```python
def test_creates_temp_table(self, project):
    table_name = f"{project.database}.{project.test_schema}.temp_table"
    project.run_sql(f"CREATE TABLE {table_name} (id INT)")
    # Test logic
```

## Debugging Tests

### IDE Test Runner (Recommended)

For debugging, use your IDE's test runner rather than pytest directly:

1. **VS Code/Cursor**: Use Test Explorer to run individual tests with debugging support
2. **Set breakpoints** in test files and step through execution
3. **Use "Python: Debug Tests"** configuration for detailed debugging

**Switching Test Environments:**
To test against different Databricks environments during IDE debugging, modify the default profile in `tests/conftest.py`:

```python
def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="databricks_cluster", type=str)
    # Change default to: "databricks_uc_cluster" or "databricks_uc_sql_endpoint"
```

### Log Analysis

Tests generate detailed debug logs by default.
Searching for red text in these logs is often sufficient to determine what has gone wrong, especially if your test failure mentions that dbt did not reach the expected state.

### Test Isolation

For command-line debugging, run individual tests:

```bash
# Single test method
pytest tests/unit/test_adapter.py::TestDatabricksAdapter::test_my_method -v

# Single test class
pytest tests/functional/adapter/basic/test_basic.py::TestBasic -v
```

## Continuous Integration

### GitHub Actions

Tests run automatically on:

- Pull requests
- Main branch commits
- Release branches

### Matrix Testing

The CI pipeline tests across:

- Multiple Python versions (3.9, 3.10, 3.11, 3.12) for unit tests
- Different Databricks environments for functional tests
- Various feature combinations

### Performance Considerations

- Unit tests run in parallel (`-n auto`)
- Functional tests are grouped by scope (`--dist=loadscope`)
- Database connections are pooled for efficiency

## Best Practices

### Test Organization

1. **Group related tests**: Use classes to group related functionality
2. **Descriptive names**: Test names should clearly describe what's being tested
3. **Minimal fixtures**: Only include necessary test data
4. **Proper scoping**: Use appropriate fixture scopes (`function`, `class`, `session`)

### Test Data

1. **Use realistic data**: Test with data that resembles production scenarios
2. **Test edge cases**: Include boundary conditions and error cases
3. **Minimize size**: Keep test datasets small for speed

### Environment Considerations

1. **Profile skipping**: Skip tests on incompatible environments
2. **Feature flags**: Test both enabled and disabled states
3. **Version compatibility**: Consider dbt Core version differences
4. **Resource management**: Be mindful of cluster resources in functional tests

### Performance

1. **Fast unit tests**: Keep unit tests under 100ms each
2. **Parallel execution**: Write tests that can run in parallel
3. **Efficient queries**: Use simple queries in functional tests

## Troubleshooting

### Common Issues

1. **Connection failures**: Check environment variables and credentials
2. **Permission errors**: Verify appropriate permissions on the test resources
3. **Unusual resources**: Some tests require you to set up an external location and give your workspace access to it

### Getting Help

- Check existing tests for patterns
- Review test logs for detailed error information
- Use debug mode for step-by-step execution

## Test Coverage

The test suite covers:

- ✅ All materialization types (table, view, incremental, snapshot)
- ✅ Python model execution and environments
- ✅ Unity Catalog features (volumes, external locations)
- ✅ Databricks-specific features (Delta Lake, liquid clustering, table properties)
- ✅ Authentication methods (token, OAuth, Azure AD)
- ✅ SQL and compute warehouse compatibility
- ✅ Error handling and edge cases

For specific test coverage reports, run:

```bash
hatch run unit-with-cov
# Opens HTML coverage report
```
