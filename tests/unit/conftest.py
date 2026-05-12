"""Stub out binary-dependent packages that can't be installed in this environment."""

import sys
from types import ModuleType


def _pkg(name: str, **attrs) -> ModuleType:
    mod = ModuleType(name)
    mod.__dict__.update(attrs)
    # Make it look like a package so sub-module attribute access works
    mod.__path__ = []
    mod.__package__ = name
    sys.modules[name] = mod
    return mod


# databricks-sql-connector requires thrift (binary wheel), which cannot be built here.
# Register minimal stubs for every sub-module the adapter imports at module level so that
# unit tests that only exercise credentials/auth can collect and run.
if "databricks.sql" not in sys.modules:
    _pkg("databricks.sql", __version__="0.0.0", connect=None)
    _pkg("databricks.sql.exc", Error=Exception, OperationalError=Exception)
    _pkg("databricks.sql.client", Connection=object, Cursor=object)
