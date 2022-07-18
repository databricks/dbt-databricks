import pytest

from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.util import AnyFloat, AnyString

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences


class TestSimpleMaterializationsDatabricks(BaseSimpleMaterializations):
    pass


class TestSingularTestsDatabricks(BaseSingularTests):
    pass


class TestSingularTestsEphemeralDatabricks(BaseSingularTestsEphemeral):
    pass


class TestEmptyDatabricks(BaseEmpty):
    pass


class TestEphemeralDatabricks(BaseEphemeral):
    pass


class TestIncrementalDatabricks(BaseIncremental):
    pass


class TestGenericTestsDatabricks(BaseGenericTests):
    pass


class TestSnapshotCheckColsDatabricks(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampDatabricks(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodDatabricks(BaseAdapterMethod):
    pass


class TestDocsGenerateDatabricks(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=AnyString(),
            id_type="long",
            text_type="string",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesDatabricks(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def model_stats(self, is_uc):
        if is_uc:
            return no_stats()
        else:
            return {
                "bytes": {
                    "description": None,
                    "id": "bytes",
                    "include": True,
                    "label": "bytes",
                    "value": AnyFloat(),
                },
                "has_stats": {
                    "id": "has_stats",
                    "label": "Has Stats?",
                    "value": True,
                    "description": "Indicates whether there are statistics for this table",
                    "include": False,
                },
            }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, model_stats):
        return expected_references_catalog(
            project,
            role=AnyString(),
            id_type="long",
            text_type="string",
            time_type="timestamp",
            bigint_type="long",
            view_type="view",
            table_type="table",
            model_stats=model_stats,
            seed_stats=no_stats(),
            view_summary_stats=no_stats(),
        )
