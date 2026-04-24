# Devcontainer for dbt-databricks-enhanced — Design

Date: 2026-04-24
Status: Approved, pending implementation

## Goal

Add a VS Code devcontainer to `dbt-databricks-enhanced` so contributors get a
reproducible, pre-wired development environment matching the existing
documented workflow (hatch + uv + ruff + mypy + pre-commit + pytest) without
manual host setup beyond Docker and VS Code.

Aligns with devcontainer patterns already used in sibling repositories
`viadukt-data-transformations` and `viadukt-databricks-notebooks`.

## Non-Goals

- Running Databricks workloads locally. Tests still talk to a remote
  Databricks workspace via `test.env`.
- Supporting IDEs other than VS Code / Cursor. The devcontainer spec is
  VS Code-centric; other IDEs may work but are not tuned for.
- Bundling zsh / powerlevel10k theming (explicitly out of scope for this
  repo, unlike the sibling repos).
- Docker Compose / multi-service setup. This repo has no local services.

## Architecture

Single-service devcontainer, no `docker-compose.yml`.

```
.devcontainer/
  devcontainer.json   # VS Code config, mounts, extensions, post-create
  Dockerfile          # Python 3.10 base + hatch + uv + Databricks CLI
```

Additional changes:
- `.gitignore` — add `.hatch/` so the in-workspace hatch data dir is ignored
- `docs/dbt-databricks-dev.md` — point contributors at the devcontainer

## Key Decisions

### Base image: Python 3.10
`mcr.microsoft.com/devcontainers/python:3.10`. Matches the default hatch
env in `pyproject.toml` (`python = "3.10"`). The matrix envs (3.11/3.12/3.13)
are installed on demand by hatch when `hatch run test:unit` is invoked
across the matrix.

### Hatch-first workflow (not raw uv)
The repo's `pyproject.toml` defines all developer tasks as `hatch run …`
(`unit`, `code-quality`, `cluster-e2e`, etc.). Hatch uses uv internally
(`installer = "uv"`). The container therefore:
- Installs `hatch` globally via pip
- Ships the `uv` binary from `ghcr.io/astral-sh/uv:latest` so hatch's
  installer works and uv is available for ad-hoc use
- Runs `hatch env create` in `postCreateCommand`

### Hatch data dir pinned to workspace
`HATCH_DATA_DIR=/workspaces/${localWorkspaceFolderBasename}/.hatch`.
Makes the virtualenv path predictable for VS Code's
`python.defaultInterpreterPath`. Without this, hatch places envs under
a user data dir that varies by platform and hashes.

### Persistence via named volumes
Rebuilding or recreating the container must not force a re-download of
every dependency. Two named Docker volumes handle this:

| Volume                                            | Mount target                                             | Purpose                       |
|---------------------------------------------------|----------------------------------------------------------|-------------------------------|
| `${localWorkspaceFolderBasename}-hatch`           | `/workspaces/<repo>/.hatch`                              | Hatch virtualenvs cache       |
| `${localWorkspaceFolderBasename}-cache`           | `/home/vscode/.cache`                                    | uv / pip / pre-commit caches  |

Named volumes (not bind mounts) so they survive container rebuilds and
aren't slowed by macOS bind-mount overhead.

### Databricks credentials
Three bind mounts expose host credentials:
- `~/.ssh` — git over SSH
- `~/.databrickscfg` — Databricks CLI config
- `~/.databricks/` — Databricks CLI state dir

The `test.env` file (already documented) is what pytest loads via
`env_files = ["test.env"]`. The `.databrickscfg` / `.databricks/` mounts
are for interactive CLI use inside the container.

### Extensions
Full set per dev docs plus useful extras:
- Core: `charliermarsh.ruff`, `ms-python.mypy-type-checker`,
  `berublan.vscode-log-viewer`, `ms-python.python`,
  `ms-python.vscode-pylance`
- Additions: `eamodio.gitlens`, `github.vscode-github-actions`,
  `editorconfig.editorconfig`, `samuelcolvin.jinjahtml`,
  `ms-toolsai.jupyter`, `vivaxy.vscode-conventional-commits`,
  `databricks.databricks`

## File Contents

### `.devcontainer/Dockerfile`

```dockerfile
FROM mcr.microsoft.com/devcontainers/python:3.10

# Databricks CLI (for interactive use inside container)
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh \
 && rm -rf /var/lib/apt/lists/*

ENV USER=vscode
ENV HOME=/home/${USER}

# uv binary — used by hatch's installer and available for ad-hoc commands
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy-mode link is safe when .cache lives on a mounted volume
ENV UV_LINK_MODE=copy

# Install hatch globally so it's on PATH for the vscode user
RUN pip install --no-cache-dir hatch

USER ${USER}
```

### `.devcontainer/devcontainer.json`

```jsonc
{
  "name": "dbt-databricks-enhanced",
  "build": {
    "dockerfile": "Dockerfile",
    "context": ".."
  },
  "remoteUser": "vscode",
  "workspaceFolder": "/workspaces/${localWorkspaceFolderBasename}",

  "containerEnv": {
    "HATCH_DATA_DIR": "/workspaces/${localWorkspaceFolderBasename}/.hatch",
    "UV_LINK_MODE": "copy"
  },

  "mounts": [
    "source=${localEnv:HOME}/.ssh,target=/home/vscode/.ssh,type=bind,consistency=cached",
    "source=${localEnv:HOME}/.databrickscfg,target=/home/vscode/.databrickscfg,type=bind,consistency=cached",
    "source=${localEnv:HOME}/.databricks,target=/home/vscode/.databricks,type=bind,consistency=cached",
    "source=${localWorkspaceFolderBasename}-hatch,target=/workspaces/${localWorkspaceFolderBasename}/.hatch,type=volume",
    "source=${localWorkspaceFolderBasename}-cache,target=/home/vscode/.cache,type=volume"
  ],

  "postCreateCommand": "sudo chown -R vscode:vscode /home/vscode/.cache /workspaces/${localWorkspaceFolderBasename}/.hatch && hatch env create && hatch run setup-precommit",
  "postStartCommand": "git config --global --add safe.directory /workspaces/${localWorkspaceFolderBasename}",

  "customizations": {
    "vscode": {
      "settings": {
        "python.defaultInterpreterPath": "/workspaces/${localWorkspaceFolderBasename}/.hatch/env/virtual/dbt-databricks-enhanced/*/bin/python",
        "mypy-type-checker.importStrategy": "fromEnvironment",
        "python.testing.unittestEnabled": false,
        "python.testing.pytestEnabled": true,
        "python.testing.pytestArgs": ["--color=yes", "-n=auto", "--dist=loadscope"],
        "[python]": {
          "editor.insertSpaces": true,
          "editor.tabSize": 4,
          "editor.formatOnSave": true,
          "editor.defaultFormatter": "charliermarsh.ruff",
          "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
          }
        },
        "logViewer.watch": [
          {
            "title": "dbt logs",
            "pattern": "${workspaceFolder}/logs/**/dbt.log"
          }
        ]
      },
      "extensions": [
        "charliermarsh.ruff",
        "ms-python.mypy-type-checker",
        "berublan.vscode-log-viewer",
        "ms-python.python",
        "ms-python.vscode-pylance",
        "eamodio.gitlens",
        "github.vscode-github-actions",
        "editorconfig.editorconfig",
        "samuelcolvin.jinjahtml",
        "ms-toolsai.jupyter",
        "vivaxy.vscode-conventional-commits",
        "databricks.databricks"
      ]
    }
  }
}
```

### `.gitignore` addition

```
.hatch/
```

## Post-Create Flow

1. Volumes mount (some owned by root initially)
2. `sudo chown -R vscode:vscode` on `.cache` and `.hatch`
3. `hatch env create` — builds default virtualenv under `$HATCH_DATA_DIR`
4. `hatch run setup-precommit` — installs pre-commit git hooks

On subsequent rebuilds the volumes are warm; step 3 is a no-op re-verify.

## Documentation Update

Add to the top of `docs/dbt-databricks-dev.md` (before "Environment Setup"):

> **Devcontainer (recommended):** Open the repo in VS Code with the
> Dev Containers extension and choose *Reopen in Container*. The
> `.devcontainer/` config installs Python 3.10, hatch, uv, and the
> Databricks CLI, then runs `hatch env create` and sets up pre-commit
> hooks. If VS Code prompts for a Python interpreter, pick the one
> under `.hatch/env/virtual/dbt-databricks-enhanced/*/bin/python`.

The existing manual-setup section stays as the fallback path.

## Failure Modes and Mitigations

| Scenario                                        | Behavior                                   | Mitigation                                        |
|-------------------------------------------------|--------------------------------------------|---------------------------------------------------|
| Host lacks `~/.databrickscfg` or `~/.databricks`| Bind-mount fails, container won't start    | Document: `touch ~/.databrickscfg && mkdir -p ~/.databricks` |
| `hatch env create` fails (network)              | postCreate fails; container still usable   | Re-run `hatch env create` manually                |
| Host UID != 1000                                | Bind-mounted `.ssh` perms may mismatch     | Standard devcontainer issue; adjust `chmod` on host |
| Interpreter path glob doesn't resolve           | VS Code prompts for interpreter selection  | Acceptable; one-time pick per container           |

## Verification

After implementation, verify:
- [ ] Cold build completes; VS Code attaches to container
- [ ] `hatch run unit` passes inside container
- [ ] `hatch run code-quality` runs (ruff + mypy)
- [ ] Ruff format-on-save works on a Python file
- [ ] `databricks --version` works
- [ ] Rebuild container → `hatch env create` completes in seconds (cache hit)
- [ ] Delete container entirely, recreate → same fast rebuild (named volumes survive)

## Out of Scope (Future Work)

- CI job that builds the devcontainer image to catch drift
- Pre-built image published to ghcr.io for faster first-open
- Multi-Python pre-install for instant matrix test runs
