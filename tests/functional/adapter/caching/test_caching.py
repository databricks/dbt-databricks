from dbt.tests.adapter.caching.test_caching import BaseCachingLowercaseModel
from dbt.tests.adapter.caching.test_caching import BaseCachingSelectedSchemaOnly
from dbt.tests.adapter.caching.test_caching import BaseCachingUppercaseModel
from dbt.tests.adapter.caching.test_caching import BaseNoPopulateCache


class TestNoPopulateCache(BaseNoPopulateCache):
    pass


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass
