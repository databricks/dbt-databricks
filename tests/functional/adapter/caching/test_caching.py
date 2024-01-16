from dbt.tests.adapter.caching.test_caching import (
    BaseCachingTest,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
    model_sql,
)

import pytest


class TestNoPopulateCache(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }

    def test_cache(self, project):
        # --no-populate-cache still allows the cache to populate all relations
        # under a schema, so the behavior here remains the same as other tests
        run_args = ["--no-populate-cache", "run"]
        self.run_and_inspect_cache(project, run_args)


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass
