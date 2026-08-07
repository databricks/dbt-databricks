"""Unit-test-only fixtures."""

from contextlib import nullcontext
from unittest import mock

import pytest
from databricks.sdk import config as _sdk_config

# databricks-sdk >=0.103 added a network probe to Config(); on older SDKs
# this symbol doesn't exist and there's nothing to stub.
_SDK_HAS_HOST_METADATA = hasattr(_sdk_config, "get_host_metadata")


@pytest.fixture(autouse=True)
def _stub_sdk_host_metadata():
    """Stub the SDK's host-metadata probe — unit tests must stay offline."""
    if not _SDK_HAS_HOST_METADATA:
        with nullcontext() as m:
            yield m
        return
    with mock.patch("databricks.sdk.config.get_host_metadata") as m:
        m.side_effect = ValueError("offline unit test")
        yield m
