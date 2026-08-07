#!/usr/bin/env bash
# Regenerate the committed lowest-direct lock from pyproject.toml.
# Run whenever pyproject.toml dependencies change. Commit the result
# in the same PR as the pyproject.toml change.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

uv pip compile pyproject.toml \
    --group test \
    --resolution lowest-direct \
    --python 3.10 \
    --universal \
    --format requirements.txt \
    --no-emit-index-url \
    --generate-hashes \
    --output-file requirements.lowest-direct.txt

echo "regenerated requirements.lowest-direct.txt"
