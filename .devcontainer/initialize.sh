#!/usr/bin/env bash
set -x

# Create the Hatch environment
hatch env create

# Install pre-commit
(hatch run setup-precommit && pre-commit gc) &

# Set up test.env file if it does not already exist
if test -f ${WORKSPACE_FOLDER}/test.env; then
    cp ${WORKSPACE_FOLDER}/test.env.example ${WORKSPACE_FOLDER}/test.env
fi

