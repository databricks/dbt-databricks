#!/usr/bin/env python3
"""Verify uv.lock records public PyPI metadata only."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PUBLIC_REGISTRY = "https://pypi.org/simple"
PUBLIC_PACKAGE_URL_PREFIX = "https://files.pythonhosted.org/packages/"
REGISTRY_PATTERN = re.compile(r'registry = "([^"]+)"')
SOURCE_REGISTRY_PATTERN = re.compile(r'(source = \{ registry = ")([^"]+)(" \})')
URL_PATTERN = re.compile(r'url = "([^"]+)"')


def check_uv_lock(lockfile: Path) -> list[str]:
    failures = []

    for line_number, line in enumerate(lockfile.read_text().splitlines(), start=1):
        for registry in REGISTRY_PATTERN.findall(line):
            if registry != PUBLIC_REGISTRY:
                failures.append(f"{lockfile}:{line_number}: registry URL is not public PyPI")

        for url in URL_PATTERN.findall(line):
            if not url.startswith(PUBLIC_PACKAGE_URL_PREFIX):
                failures.append(f"{lockfile}:{line_number}: package URL is not public PyPI")

    return failures


def fix_uv_lock(lockfile: Path) -> int:
    contents = lockfile.read_text()
    change_count = 0

    def normalize_registry(match: re.Match[str]) -> str:
        nonlocal change_count
        if match.group(2) == PUBLIC_REGISTRY:
            return match.group(0)
        change_count += 1
        return f"{match.group(1)}{PUBLIC_REGISTRY}{match.group(3)}"

    normalized = SOURCE_REGISTRY_PATTERN.sub(normalize_registry, contents)
    if change_count:
        lockfile.write_text(normalized)
    return change_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fix",
        action="store_true",
        help="normalize source registries before checking the lock file",
    )
    parser.add_argument("lockfile", nargs="?", type=Path, default=Path("uv.lock"))
    args = parser.parse_args()

    lockfile = args.lockfile
    if not lockfile.exists():
        print(f"{lockfile} not found", file=sys.stderr)
        return 1

    if args.fix:
        change_count = fix_uv_lock(lockfile)
        print(f"Normalized {change_count} source registry entries in {lockfile}", flush=True)

    failures = check_uv_lock(lockfile)
    if failures:
        print("uv.lock contains non-public PyPI metadata:", file=sys.stderr)
        for failure in failures[:20]:
            print(f"  {failure}", file=sys.stderr)
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more", file=sys.stderr)
        return 1

    print("uv.lock public PyPI URL check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
