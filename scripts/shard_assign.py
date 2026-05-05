#!/usr/bin/env python3
"""Assign pytest test nodeids to N shards for parallel matrix execution.

Reads a list of pytest nodeids (one per line, format `path/to/test_x.py::Class::method`)
and partitions them into N shard files plus a manifest JSON describing the
assignment.

Sharding granularity is **file-level**: all tests in the same `.py` file land
in the same shard. This preserves pytest-xdist `--dist=loadfile` semantics
(class-level setup/teardown stays on one worker) and avoids splitting tests
within a class that may share cluster state.

Algorithms (selected via --algo):
- hash_mod (default): shard = sha256(file_path) mod N. Stable, no inputs needed.
- (future) time_weighted: takes a historical-timings JSON, packs files into
  shards to balance total runtime. Not implemented in this version.

Outputs into --output-dir:
- <profile>-shard-<i>.txt : nodeid list, one per line, for i in 0..N-1
- <profile>-manifest.json : structured assignment record (see below)

Manifest schema:
{
  "profile": str,
  "num_shards": int,
  "algorithm": str,
  "total_tests": int,
  "total_files": int,
  "shards": [
    {"index": int, "tests": int, "files": int, "file_list_sha256": str},
    ...
  ],
  "all_nodeids_sha256": str
}

The two sha256 fields let the gather phase verify (a) the files-per-shard set
hasn't drifted (e.g., a worker writing to the wrong shard file) and (b) the
full nodeid universe matches what was assigned (no tests added/removed
between assignment and execution).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def hash_mod_shard(file_path: str, num_shards: int) -> int:
    """Default algorithm: shard = sha256(file_path) interpreted as a big int,
    mod N. Stable across runs."""
    digest_int = int(hashlib.sha256(file_path.encode("utf-8")).hexdigest(), 16)
    return digest_int % num_shards


ALGORITHMS = {
    "hash_mod": hash_mod_shard,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--profile", required=True, help="Profile key (e.g. databricks_uc_cluster)")
    p.add_argument("--num-shards", type=int, required=True, help="Number of shards")
    p.add_argument("--input", required=True, help="File of pytest nodeids, one per line")
    p.add_argument("--output-dir", required=True, help="Directory to write shard files + manifest")
    p.add_argument(
        "--algo",
        default="hash_mod",
        choices=sorted(ALGORITHMS.keys()),
        help="Shard assignment algorithm",
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

    # Sort everything for deterministic output regardless of input order
    sorted_files = sorted(file_to_tests.keys())
    for fp in sorted_files:
        file_to_tests[fp].sort()
    all_nodeids_sorted = sorted(all_nodeids)

    # Apply algorithm
    algo_fn = ALGORITHMS[args.algo]
    shards_files: list[list[str]] = [[] for _ in range(args.num_shards)]
    shards_tests: list[list[str]] = [[] for _ in range(args.num_shards)]
    for fp in sorted_files:
        idx = algo_fn(fp, args.num_shards)
        shards_files[idx].append(fp)
        shards_tests[idx].extend(file_to_tests[fp])

    # Sort each shard's tests for deterministic output
    for s in shards_tests:
        s.sort()

    # Write per-shard nodeid files
    for i, tests in enumerate(shards_tests):
        out = output_dir / f"{args.profile}-shard-{i}.txt"
        out.write_text("\n".join(tests) + ("\n" if tests else ""))

    # Build manifest
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
                "file_list_sha256": sha256_str("\n".join(sorted(shards_files[i]))),
            }
            for i in range(args.num_shards)
        ],
        "all_nodeids_sha256": sha256_str("\n".join(all_nodeids_sorted)),
    }
    manifest_path = output_dir / f"{args.profile}-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Sanity: every test got assigned exactly once
    sum_tests = sum(s["tests"] for s in manifest["shards"])
    if sum_tests != manifest["total_tests"]:
        print(
            f"ERROR: assignment lost tests: total={manifest['total_tests']} "
            f"assigned={sum_tests}",
            file=sys.stderr,
        )
        return 1

    # Print human summary
    print(f"profile={args.profile} algo={args.algo} num_shards={args.num_shards}")
    print(f"  total: {manifest['total_tests']} tests across {manifest['total_files']} files")
    for s in manifest["shards"]:
        pct = s["tests"] / manifest["total_tests"] * 100
        print(
            f"  shard {s['index']}: {s['tests']:>4} tests ({pct:5.1f}%), "
            f"{s['files']:>3} files"
        )
    print(f"  manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
