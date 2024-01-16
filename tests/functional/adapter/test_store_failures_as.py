from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsExceptions,
)


class TestDatabricksStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions):
    pass


class TestDatabricksStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff):
    pass


class TestDatabricksStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView):
    pass


class TestDatabricksStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral
):
    pass


class TestDatabricksStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric):
    pass


class TestDatabricksStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions):
    pass
