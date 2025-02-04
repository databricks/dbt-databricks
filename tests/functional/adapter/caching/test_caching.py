from dbt.tests import util
from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingUppercaseModel,
    BaseNoPopulateCache,
)


class DatabricksNoPopulateCache:
    def run_and_inspect_cache(self, project, run_args=None):
        util.run_dbt(run_args)

        # the cache was empty at the start of the run.
        # the model materialization returned an unquoted relation and added to the cache.
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        relation = list(adapter.cache.relations).pop()
        assert relation.schema == project.test_schema
        assert relation.schema == project.test_schema.lower()

        # on the second run, dbt will find a relation in the database during cache population.
        # this relation will be quoted, because list_relations_without_caching (by default) uses
        # quote_policy = {"database": True, "schema": True, "identifier": True}
        # when adding relations to the cache.
        util.run_dbt(run_args)
        adapter = project.adapter

        # For Databricks, the cache
        assert len(adapter.cache.relations) == 2
        second_relation = list(adapter.cache.relations).pop()

        # perform a case-insensitive + quote-insensitive comparison
        for key in ["database", "schema", "identifier"]:
            assert getattr(relation, key).lower() == getattr(second_relation, key).lower()


class TestNoPopulateCache(BaseNoPopulateCache):
    pass


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass
