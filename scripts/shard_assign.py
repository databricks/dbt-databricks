#!/usr/bin/env python3
"""Partition pytest test FILES (not individual tests) into N shards for
matrix execution.

Reads pytest --collect-only nodeids, groups by file, assigns each file to a
shard via the chosen algorithm, and writes a per-shard list of file paths
(consumed by `xargs ... pytest < shard.txt`). File-level granularity
preserves --dist=loadfile fixture sharing; passing file paths (not nodeids)
keeps pytest's file-declaration order intact (some test classes have
order-dependent fixtures).

Algorithms:
- lpt_historical_time: greedy LPT on historical per-file wall time. Reads
  --timings (built by scripts/refresh_timings.py); falls back to per-profile
  mean for files not in the timings.
- lpt_test_count (default): greedy LPT on per-file test count. No external
  data needed.

Outputs `<profile>-shard-<i>.txt` (file paths) and `<profile>-manifest.json`
(includes per-shard nodeids for shard_verify, plus sha256 checksums).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def lpt_test_count_assign(file_to_tests: dict[str, list[str]], num_shards: int) -> dict[str, int]:
    """Greedy LPT weighted by per-file test count."""
    return _greedy_lpt(file_to_tests, num_shards, lambda fp: len(file_to_tests[fp]))


def make_historical_weight(timings: dict[str, float]) -> Callable[[str], float]:
    """Per-file wall-time weight fn: known files use their timing, unknown
    (e.g. brand-new tests) use the profile mean."""
    if not timings:
        raise ValueError("lpt_historical_time requires non-empty timings dict")
    mean = sum(timings.values()) / len(timings)
    return lambda fp: timings.get(fp, mean)


def make_lpt_historical_time_assign(
    timings: dict[str, float],
) -> Callable[[dict[str, list[str]], int], dict[str, int]]:
    """Greedy LPT weighted by historical per-file wall time. Files missing
    from `timings` use the mean — handles brand-new tests."""
    weight = make_historical_weight(timings)

    def assign(file_to_tests: dict[str, list[str]], num_shards: int) -> dict[str, int]:
        return _greedy_lpt(file_to_tests, num_shards, weight)

    return assign


def _greedy_lpt(
    file_to_tests: dict[str, list[str]],
    num_shards: int,
    weight: Callable[[str], float],
) -> dict[str, int]:
    # Materialize weights once: every file is weighed exactly once even if
    # `weight` is impure or expensive.
    weights = {fp: weight(fp) for fp in file_to_tests}
    files_sorted = sorted(weights, key=lambda fp: (-weights[fp], fp))
    shards_load: list[float] = [0.0] * num_shards
    out: dict[str, int] = {}
    for fp in files_sorted:
        idx = min(range(num_shards), key=lambda i: (shards_load[i], i))
        out[fp] = idx
        shards_load[idx] += weights[fp]
    return out


ALGORITHM_CHOICES = ("lpt_historical_time", "lpt_test_count")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", required=True, help="Profile key (e.g. databricks_uc_cluster)")
    p.add_argument("--num-shards", type=int, required=True, help="Number of shards")
    p.add_argument("--input", required=True, help="File of pytest nodeids, one per line")
    p.add_argument("--output-dir", required=True, help="Directory to write shard files + manifest")
    p.add_argument(
        "--algo",
        default="lpt_test_count",
        choices=ALGORITHM_CHOICES,
        help="Shard assignment algorithm (default: lpt_test_count)",
    )
    p.add_argument(
        "--timings",
        default=None,
        help=(
            "Path to test_timings.json (built by refresh_timings.py). "
            "Required when --algo is lpt_historical_time."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.num_shards < 1:
        print("ERROR: --num-shards must be >= 1", file=sys.stderr)
        return 2

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse nodeids; group by file
    file_to_tests: dict[str, list[str]] = defaultdict(list)
    all_nodeids: list[str] = []
    with input_path.open() as f:
        for raw in f:
            nodeid = raw.strip()
            if not nodeid or "::" not in nodeid:
                continue
            file_path = nodeid.split("::", 1)[0]
            file_to_tests[file_path].append(nodeid)
            all_nodeids.append(nodeid)

    if not all_nodeids:
        print(f"ERROR: no valid nodeids in {input_path}", file=sys.stderr)
        return 2

    sorted_files = sorted(file_to_tests.keys())

    weight: Callable[[str], float] | None = None
    if args.algo == "lpt_historical_time":
        if not args.timings:
            print("ERROR: --algo lpt_historical_time requires --timings PATH", file=sys.stderr)
            return 2
        timings_path = Path(args.timings)
        if not timings_path.exists():
            print(f"ERROR: timings file not found: {timings_path}", file=sys.stderr)
            return 2
        timings_doc = json.loads(timings_path.read_text())
        profile_timings = timings_doc.get(args.profile, {})
        if not profile_timings:
            print(
                f"ERROR: no timings for profile '{args.profile}' in {timings_path}. "
                f"Run scripts/refresh_timings.py for this profile first.",
                file=sys.stderr,
            )
            return 2
        weight = make_historical_weight(profile_timings)
        algo_fn = make_lpt_historical_time_assign(profile_timings)
    else:
        algo_fn = lpt_test_count_assign
    file_to_shard = algo_fn(file_to_tests, args.num_shards)
    shards_files: list[list[str]] = [[] for _ in range(args.num_shards)]
    shards_tests: list[list[str]] = [[] for _ in range(args.num_shards)]
    for fp in sorted_files:
        idx = file_to_shard[fp]
        shards_files[idx].append(fp)
        shards_tests[idx].extend(file_to_tests[fp])

    # Write per-shard FILE-PATH files (consumed by `xargs ... pytest < shard.txt`)
    for i, files in enumerate(shards_files):
        out = output_dir / f"{args.profile}-shard-{i}.txt"
        out.write_text("\n".join(files) + ("\n" if files else ""))

    # Projected per-shard wall seconds under the chosen weights. Only meaningful
    # for lpt_historical_time (test-count weights aren't wall time), so it's
    # emitted only then. The weekly refresh job (refresh-test-timings.yml) reads
    # `projected_max_shard_seconds` to gate on balance drift.
    projected_shard_seconds: list[float] | None = None
    if weight is not None:
        projected_shard_seconds = [
            round(sum(weight(fp) for fp in shards_files[i]), 3) for i in range(args.num_shards)
        ]

    # Build manifest. The per-shard `nodeids` field is the source of truth
    # used by shard_verify to compute the I1-I4 invariants.
    manifest = {
        "profile": args.profile,
        "num_shards": args.num_shards,
        "algorithm": args.algo,
        "total_tests": len(all_nodeids),
        "total_files": len(sorted_files),
        "shards": [
            {
                "index": i,
                "tests": len(shards_tests[i]),
                "files": len(shards_files[i]),
                "file_list": shards_files[i],
                "nodeids": shards_tests[i],
                "file_list_sha256": sha256_str("\n".join(sorted(shards_files[i]))),
                **(
                    {"projected_seconds": projected_shard_seconds[i]}
                    if projected_shard_seconds is not None
                    else {}
                ),
            }
            for i in range(args.num_shards)
        ],
        "all_nodeids_sha256": sha256_str("\n".join(sorted(all_nodeids))),
    }
    if projected_shard_seconds is not None:
        manifest["projected_max_shard_seconds"] = max(projected_shard_seconds)
    manifest_path = output_dir / f"{args.profile}-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Sanity: every test got assigned exactly once
    sum_tests = sum(s["tests"] for s in manifest["shards"])
    if sum_tests != manifest["total_tests"]:
        print(
            f"ERROR: assignment lost tests: total={manifest['total_tests']} assigned={sum_tests}",
            file=sys.stderr,
        )
        return 1

    # Print human summary
    print(f"profile={args.profile} algo={args.algo} num_shards={args.num_shards}")
    print(f"  total: {manifest['total_tests']} tests across {manifest['total_files']} files")
    for s in manifest["shards"]:
        pct = s["tests"] / manifest["total_tests"] * 100
        proj = f", ~{s['projected_seconds'] / 60:.1f}m" if "projected_seconds" in s else ""
        print(
            f"  shard {s['index']}: {s['tests']:>4} tests ({pct:5.1f}%), "
            f"{s['files']:>3} files{proj}"
        )
    if "projected_max_shard_seconds" in manifest:
        print(f"  projected max-shard wall: {manifest['projected_max_shard_seconds'] / 60:.1f}m")
    print(f"  manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
