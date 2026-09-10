import importlib.util
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

_SCRIPT = _SCRIPTS_DIR / "check_uv_lock_public_urls.py"
_spec = importlib.util.spec_from_file_location("check_uv_lock_public_urls", _SCRIPT)
assert _spec is not None and _spec.loader is not None
checker = importlib.util.module_from_spec(_spec)
sys.modules["check_uv_lock_public_urls"] = checker
_spec.loader.exec_module(checker)

MIRROR = "https://pypi-proxy.example.com"
PACKAGE_PATH = "/packages/29/77/6f5df1c68bf/agate-1.9.1.tar.gz"

PUBLIC_LOCK = f"""\
[[package]]
name = "agate"
version = "1.9.1"
source = {{ registry = "https://pypi.org/simple" }}
sdist = {{ url = "https://files.pythonhosted.org{PACKAGE_PATH}", hash = "sha256:abc" }}
"""

MIRROR_LOCK = f"""\
[[package]]
name = "agate"
version = "1.9.1"
source = {{ registry = "{MIRROR}/simple" }}
sdist = {{ url = "{MIRROR}{PACKAGE_PATH}", hash = "sha256:abc" }}
"""


class TestCheckUvLock:
    def test_public_lock_passes(self, tmp_path):
        lockfile = tmp_path / "uv.lock"
        lockfile.write_text(PUBLIC_LOCK)
        assert checker.check_uv_lock(lockfile) == []

    def test_mirror_registry_and_url_both_reported(self, tmp_path):
        lockfile = tmp_path / "uv.lock"
        lockfile.write_text(MIRROR_LOCK)
        failures = checker.check_uv_lock(lockfile)
        assert len(failures) == 2
        assert any("registry URL is not public PyPI" in failure for failure in failures)
        assert any("package URL is not public PyPI" in failure for failure in failures)


class TestFixUvLock:
    def test_fix_normalizes_registry_and_package_url(self, tmp_path):
        lockfile = tmp_path / "uv.lock"
        lockfile.write_text(MIRROR_LOCK)

        registry_count, url_count = checker.fix_uv_lock(lockfile)

        assert (registry_count, url_count) == (1, 1)
        assert lockfile.read_text() == PUBLIC_LOCK
        assert checker.check_uv_lock(lockfile) == []

    def test_fix_is_a_no_op_on_a_public_lock(self, tmp_path):
        lockfile = tmp_path / "uv.lock"
        lockfile.write_text(PUBLIC_LOCK)

        assert checker.fix_uv_lock(lockfile) == (0, 0)
        assert lockfile.read_text() == PUBLIC_LOCK

    def test_fix_preserves_the_artifact_path_and_hash(self, tmp_path):
        """Only the host may change: the /packages/ path identifies the artifact and the hash
        next to it is what actually guarantees integrity."""
        lockfile = tmp_path / "uv.lock"
        lockfile.write_text(MIRROR_LOCK)

        checker.fix_uv_lock(lockfile)

        contents = lockfile.read_text()
        assert PACKAGE_PATH in contents
        assert 'hash = "sha256:abc"' in contents

    def test_fix_leaves_urls_that_are_not_mirror_shaped(self, tmp_path):
        """A URL without a /packages/ path is not a mirror of the public artifact tree, so
        rewriting its host would be a guess. Leave it for the check to report."""
        lockfile = tmp_path / "uv.lock"
        original = 'sdist = { url = "https://example.com/downloads/agate-1.9.1.tar.gz" }\n'
        lockfile.write_text(original)

        assert checker.fix_uv_lock(lockfile) == (0, 0)
        assert lockfile.read_text() == original
        assert len(checker.check_uv_lock(lockfile)) == 1
