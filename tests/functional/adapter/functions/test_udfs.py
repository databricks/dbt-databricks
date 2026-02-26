import pytest
from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
)
from dbt.tests.util import run_dbt, write_file
from dbt_common.events.base_types import EventMsg
from dbt_common.events.event_catcher import EventCatcher

DATABRICKS_PYTHON_UDF_BODY = """
def price_for_xlarge(price):
    return price * 2

return price_for_xlarge(price)
"""

# Databricks-compatible Python UDF YAML (using Python 3.11)
DATABRICKS_PYTHON_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
    returns:
      data_type: float
      description: The resulting xlarge price
"""


DATABRICKS_MULTI_ARG_PYTHON_UDF_BODY = """
def total_price(price, quantity):
    return price * quantity

return total_price(price, quantity)
"""

DATABRICKS_MULTI_ARG_PYTHON_UDF_YML = """
functions:
  - name: total_price
    description: Calculate total price from unit price and quantity
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: int
    returns:
      data_type: float
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksUDFs(UDFsBasic):
    """Basic SQL UDF test - requires Unity Catalog"""

    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksPythonUDFSupported(UDFsBasic):
    """Test that Python UDFs work on Databricks with Unity Catalog.

    Verifies:
    - Python UDF creates successfully
    - Generated SQL contains LANGUAGE PYTHON
    - Dollar-quoting is used for the function body
    - The UDF executes correctly and returns expected results
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.py": DATABRICKS_PYTHON_UDF_BODY,
            "price_for_xlarge.yml": DATABRICKS_PYTHON_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR REPLACE FUNCTION" in event.data.sql
        )

    def test_udfs(self, project, sql_event_catcher):
        """Test Python UDF creation and execution on Databricks."""
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        # Verify build succeeded
        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Verify the generated SQL contains Databricks Python UDF syntax
        assert len(sql_event_catcher.caught_events) == 1
        generated_sql = sql_event_catcher.caught_events[0].data.sql

        # Check for LANGUAGE PYTHON
        assert "LANGUAGE PYTHON" in generated_sql, (
            f"Expected 'LANGUAGE PYTHON' in generated SQL:\n{generated_sql}"
        )

        # Verify RUNTIME_VERSION and HANDLER are NOT in the generated SQL
        # (Databricks does not support these clauses for Python UDFs)
        assert "RUNTIME_VERSION" not in generated_sql, (
            f"Unexpected 'RUNTIME_VERSION' in generated SQL:\n{generated_sql}"
        )
        assert "HANDLER" not in generated_sql, (
            f"Unexpected 'HANDLER' in generated SQL:\n{generated_sql}"
        )

        # Check for dollar-quoting
        assert "$$" in generated_sql, (
            f"Expected dollar-quoting '$$' in generated SQL:\n{generated_sql}"
        )

        # Verify the UDF actually works by executing it
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200, f"Expected 200, got {select_value}"


# Python UDF with different implementation for update testing (inline code)
PYTHON_UDF_V1 = """
return price * 2
"""

PYTHON_UDF_V2 = """
return price * 3
"""

PYTHON_UDF_YML_V1 = """
functions:
  - name: price_for_xlarge
    config:
      entry_point: price_for_xlarge
      runtime_version: "3.11"
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksPythonUDFLifecycle:
    """Test the full lifecycle of a Python UDF.

    Verifies:
    1. Initial create - function is created successfully
    2. Subsequent runs - function is replaced (idempotent)
    3. Code change - function is updated with new implementation
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.py": PYTHON_UDF_V1,
            "price_for_xlarge.yml": PYTHON_UDF_YML_V1,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR REPLACE FUNCTION" in event.data.sql
        )

    @pytest.fixture(scope="class")
    def sql_event_catcher(self) -> EventCatcher:
        return EventCatcher(
            event_to_catch=SQLQuery, predicate=lambda event: self.is_function_create_event(event)
        )

    def test_python_udf_lifecycle(self, project, sql_event_catcher):
        """Test the full lifecycle of a Python UDF."""

        # ===========================================
        # Phase 1: Initial Create
        # ===========================================
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify initial SQL has LANGUAGE PYTHON
        assert len(sql_event_catcher.caught_events) == 1
        initial_sql = sql_event_catcher.caught_events[0].data.sql
        assert "LANGUAGE PYTHON" in initial_sql

        # Verify function works: price * 2
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        # ===========================================
        # Phase 2: Subsequent Run (Idempotent)
        # ===========================================
        sql_event_catcher.caught_events.clear()
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # CREATE OR REPLACE is called again
        assert len(sql_event_catcher.caught_events) == 1

        # Function still works the same way
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        # ===========================================
        # Phase 3: Code Change (price * 2 -> price * 3)
        # ===========================================
        # Update the Python file
        write_file(PYTHON_UDF_V2, project.project_root, "functions", "price_for_xlarge.py")

        sql_event_catcher.caught_events.clear()
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify new implementation works: price * 3
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 300


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksMultiArgPythonUDF(UDFsBasic):
    """Test that Python UDFs with multiple arguments work on Databricks.

    Verifies:
    - Multi-arg Python UDF creates successfully
    - Generated SQL contains both arguments in the signature
    - The UDF executes correctly with multiple arguments
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "total_price.py": DATABRICKS_MULTI_ARG_PYTHON_UDF_BODY,
            "total_price.yml": DATABRICKS_MULTI_ARG_PYTHON_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "total_price"
            and "CREATE OR REPLACE FUNCTION" in event.data.sql
        )

    def test_udfs(self, project, sql_event_catcher):
        """Test multi-arg Python UDF creation and execution on Databricks."""
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        # Verify build succeeded
        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "total_price"

        # Verify the generated SQL contains Databricks Python UDF syntax
        assert len(sql_event_catcher.caught_events) == 1
        generated_sql = sql_event_catcher.caught_events[0].data.sql

        # Check for LANGUAGE PYTHON and dollar-quoting
        assert "LANGUAGE PYTHON" in generated_sql, (
            f"Expected 'LANGUAGE PYTHON' in generated SQL:\n{generated_sql}"
        )
        assert "$$" in generated_sql, (
            f"Expected dollar-quoting '$$' in generated SQL:\n{generated_sql}"
        )

        # Verify both arguments appear in the signature
        assert "price" in generated_sql, (
            f"Expected 'price' in generated SQL:\n{generated_sql}"
        )
        assert "quantity" in generated_sql, (
            f"Expected 'quantity' in generated SQL:\n{generated_sql}"
        )

        # Verify the UDF actually works by executing it with two arguments
        result = run_dbt(
            ["show", "--inline", "SELECT {{ function('total_price') }}(25.0, 4)"]
        )
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 100, f"Expected 100, got {select_value}"
