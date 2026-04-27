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
