#!/usr/bin/env python3
"""Verify uv.lock records public PyPI metadata only."""

from __future__ import annotations

import re
import sys
from pathlib import Path


PUBLIC_REGISTRY = "https://pypi.org/simple"
PUBLIC_PACKAGE_URL_PREFIX = "https://files.pythonhosted.org/packages/"
REGISTRY_PATTERN = re.compile(r'registry = "([^"]+)"')
URL_PATTERN = re.compile(r'url = "([^"]+)"')


def check_uv_lock(lockfile: Path) -> list[str]:
    failures = []

    for line_number, line in enumerate(lockfile.read_text().splitlines(), start=1):
        for registry in REGISTRY_PATTERN.findall(line):
            if registry != PUBLIC_REGISTRY:
                failures.append(
                    f"{lockfile}:{line_number}: registry URL is not {PUBLIC_REGISTRY}"
                )

        for url in URL_PATTERN.findall(line):
            if not url.startswith(PUBLIC_PACKAGE_URL_PREFIX):
                failures.append(
                    f"{lockfile}:{line_number}: package URL is not under {PUBLIC_PACKAGE_URL_PREFIX}"
                )

    return failures


def main() -> int:
    lockfile = Path("uv.lock")
    if not lockfile.exists():
        print("uv.lock not found", file=sys.stderr)
        return 1

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
