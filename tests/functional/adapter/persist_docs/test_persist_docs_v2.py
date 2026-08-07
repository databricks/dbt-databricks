import pytest

from tests.functional.adapter.fixtures import MaterializationV2Mixin
from tests.functional.adapter.persist_docs.test_persist_docs import (
    TestPersistDocs as _TestPersistDocs,
)


class TestPersistDocsV2(_TestPersistDocs, MaterializationV2Mixin):
    """V2 persist_docs on the table/view model create path.

    TestPersistDocs covers V1; the only pre-existing V2 coverage is
    TestPersistDocsWithSeedsV2 (the seed path). This class reuses the V1 models,
    properties, and assertions, adding the use_materialization_v2 flag so comments
    flow through relations/table/create.sql and relations/view/create.sql.

    project_config_update is overridden (rather than relying on
    MaterializationV2Mixin) because the V1 base already defines it; the override
    merges the V1 persist_docs config with the V2 flag.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }
