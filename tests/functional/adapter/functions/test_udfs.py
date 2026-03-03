import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
)
from dbt.tests.util import run_dbt, write_file

from tests.functional.adapter.functions.fixtures import (
    DATABRICKS_MULTI_ARG_PYTHON_UDF_BODY,
    DATABRICKS_MULTI_ARG_PYTHON_UDF_YML,
    DATABRICKS_PYTHON_UDF_BODY,
    DATABRICKS_PYTHON_UDF_YML,
    PYTHON_UDF_V1,
    PYTHON_UDF_V2,
    PYTHON_UDF_YML_V1,
)


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksUDFs(UDFsBasic):
    """Basic SQL UDF test - requires Unity Catalog"""

    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestDatabricksPythonUDFSupported(UDFsBasic):
    """Test that Python UDFs work on Databricks with Unity Catalog.

    Verifies:
    - Python UDF creates successfully
    - The UDF executes correctly and returns expected results
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.py": DATABRICKS_PYTHON_UDF_BODY,
            "price_for_xlarge.yml": DATABRICKS_PYTHON_UDF_YML,
        }

    def test_udfs(self, project):
        """Test Python UDF creation and execution on Databricks."""
        result = run_dbt(["build"])

        # Verify build succeeded
        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Verify the UDF actually works by executing it
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200, f"Expected 200, got {select_value}"


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

    def test_python_udf_lifecycle(self, project):
        """Test the full lifecycle of a Python UDF."""

        # ===========================================
        # Phase 1: Initial Create
        # ===========================================
        result = run_dbt(["build"])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Verify function works: price * 2
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        # ===========================================
        # Phase 2: Subsequent Run (Idempotent)
        # ===========================================
        result = run_dbt(["build"])

        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success

        # Function still works the same way
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        # ===========================================
        # Phase 3: Code Change (price * 2 -> price * 3)
        # ===========================================
        # Update the Python file
        write_file(PYTHON_UDF_V2, project.project_root, "functions", "price_for_xlarge.py")

        result = run_dbt(["build"])

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
    - The UDF executes correctly with multiple arguments
    """

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "total_price.py": DATABRICKS_MULTI_ARG_PYTHON_UDF_BODY,
            "total_price.yml": DATABRICKS_MULTI_ARG_PYTHON_UDF_YML,
        }

    def test_udfs(self, project):
        """Test multi-arg Python UDF creation and execution on Databricks."""
        result = run_dbt(["build"])

        # Verify build succeeded
        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)
        assert node_result.node.name == "total_price"

        # Verify the UDF actually works by executing it with two arguments
        result = run_dbt(["show", "--inline", "SELECT {{ function('total_price') }}(25.0, 4)"])
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 100, f"Expected 100, got {select_value}"
