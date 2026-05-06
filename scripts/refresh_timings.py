#!/usr/bin/env python3
"""Aggregate per-file historical wall-time from a previous run's pytest junit
XMLs into `scripts/test_timings.json`. Used as input to `shard_assign.py`'s
`lpt_historical_time` algorithm so file partitioning balances on actual
runtime instead of test count.

Usage (one profile per invocation; updates the JSON in place):

  # 1) Download junit XMLs for one profile from a previous green run
  gh run download <run_id> --repo databricks/dbt-databricks \\
    -p '*-test-logs-*-shard-*' -D /tmp/junits

  # 2) Aggregate into scripts/test_timings.json
  python scripts/refresh_timings.py \\
    --profile databricks_cluster \\
    --output scripts/test_timings.json \\
    /tmp/junits/cluster-test-logs-1438-shard-*/junit-shard-*.xml

The output JSON has a profile-keyed dict of file_path -> cumulative wall (sec):

  {
    "_meta": {"generated_at": "<iso>", "source": {<profile>: "<note>"}},
    "databricks_cluster": {
      "tests/functional/adapter/python_model/test_python_model.py": 2832.5,
      ...
    },
    ...
  }

`shard_assign.py` reads this and uses it as file weights for greedy LPT.
For files not present in the timings (e.g. brand-new tests), it falls back
to the mean per-file time within that profile.
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def classname_to_file(classname: str) -> str:
    """Convert a pytest junit `classname` (dotted module path + class chain)
    into a file path. The file is the segment ending in the last `test_*`
    component; everything after that is class chain.

    Examples:
      tests.functional.adapter.grants.test_grants.TestModelGrants
        → tests/functional/adapter/grants/test_grants.py
      tests.unit.test_x  (module-level test, no class)
        → tests/unit/test_x.py
    """
    parts = classname.split(".")
    # Walk from the right; the LAST test_* segment marks the file boundary.
    last_test_idx = max(
        (i for i, p in enumerate(parts) if p.startswith("test_")),
        default=-1,
    )
    if last_test_idx == -1:
        return "/".join(parts) + ".py"
    return "/".join(parts[: last_test_idx + 1]) + ".py"


def aggregate_per_file(junit_paths: list[Path]) -> dict[str, float]:
    """Sum `time` attributes across all testcases per file."""
    by_file: dict[str, float] = defaultdict(float)
    for jp in junit_paths:
        for tc in ET.parse(jp).getroot().findall(".//testcase"):
            f = classname_to_file(tc.get("classname", ""))
            by_file[f] += float(tc.get("time", "0") or 0)
    return dict(by_file)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", required=True, help="Profile key (e.g. databricks_uc_cluster)")
    p.add_argument(
        "--output",
        required=True,
        help="Path to test_timings.json (read-modify-write; created if missing)",
    )
    p.add_argument(
        "--source-note",
        default="",
        help="Free-form note recorded under _meta.source[profile] (e.g. 'exp-9 run 25418581604')",
    )
    p.add_argument("junits", nargs="+", help="Junit XML files for this profile")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    junit_paths = [Path(j) for j in args.junits]
    missing = [j for j in junit_paths if not j.exists()]
    if missing:
        print(f"ERROR: junit files not found: {missing}", file=sys.stderr)
        return 2

    by_file = aggregate_per_file(junit_paths)
    if not by_file:
        print("ERROR: no testcases parsed from any junit XML", file=sys.stderr)
        return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        existing = json.loads(output_path.read_text())
    else:
        existing = {"_meta": {"source": {}}}

    existing["_meta"]["generated_at"] = datetime.now(timezone.utc).isoformat()
    existing["_meta"].setdefault("source", {})
    if args.source_note:
        existing["_meta"]["source"][args.profile] = args.source_note
    # Round to ms for compactness; per-test variance is far above 1 ms anyway.
    existing[args.profile] = {fp: round(t, 3) for fp, t in sorted(by_file.items())}

    output_path.write_text(json.dumps(existing, indent=2, sort_keys=False) + "\n")

    total = sum(by_file.values())
    print(f"profile={args.profile} files={len(by_file)} total_wall={total / 60:.1f}m")
    print(f"  written: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
