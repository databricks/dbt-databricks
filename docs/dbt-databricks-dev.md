# Developing dbt-databricks

This guide covers the essential setup and workflow for developing the dbt-databricks adapter.

## Quick Start

### 1. Environment Setup

**Install Hatch** (recommended):

```bash
# Install Hatch globally - see https://hatch.pypa.io/dev/install/
pip install hatch

# Create default environment
hatch env create
```

**IDE Integration**:
Set your IDE's Python interpreter to `.hatch/dbt-databricks/bin/python`

### 2. Essential Commands

```bash
hatch run code-quality           # Format, lint, type-check
hatch run unit                   # Run unit tests
hatch run cluster-e2e            # Run functional tests
```

## Development Workflow

### Making Changes

1. **Create a feature branch**:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following established patterns

3. **Run code quality checks**:

   ```bash
   hatch run code-quality
   ```

4. **Run relevant tests**:

   ```bash
   hatch run unit                    # Always run unit tests
   hatch run cluster-e2e             # Run functional tests if adapter changes
   ```

5. **Update documentation** if needed (code comments, this guide)

### Pull Request Process

**Before submitting:**

- [ ] All tests pass locally
- [ ] Code quality checks pass
- [ ] Code comments updated if needed

**After PR creation:**

- [ ] Update CHANGELOG.md for user-facing changes (requires PR link)

**Breaking changes require:**

- GitHub Issue for design discussion first
- Implementation behind behavior flag (see existing flags in `behaviors/`)
- Clear migration documentation

## Key Architecture

dbt-databricks extends [dbt-spark](https://github.com/dbt-labs/dbt-spark) with Databricks-specific features:

- **Authentication**: OAuth (U2M/M2M), Personal Access Tokens, Azure Service Principal
- **Materializations**: `table`, `view`, `incremental`, `materialized_view`, `streaming_table`
- **Incremental Strategies**: `append`, `merge`, `insert_overwrite`, `replace_where`, `microbatch`
- **Unity Catalog**: 3-level namespace, governance, grants
- **Python Models**: Execution on clusters, serverless, workflows
- **Behavior Flags**: Feature toggles (see `behaviors/` directory)

## VS Code Settings

Recommended `.vscode/settings.json`:

```json
{
  "mypy-type-checker.importStrategy": "fromEnvironment",
  "python.testing.unittestEnabled": false,
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["--color=yes", "-n=auto", "--dist=loadscope"],
  "[python]": {
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.codeActionsOnSave": {
      "source.organizeImports": "explicit"
    }
  }
}
```

## Documentation

**Two types of documentation:**

1. **Development docs** (this repo): Architecture, testing, contributing
2. **User docs** ([docs.getdbt.com](https://docs.getdbt.com)): Features, configuration, usage

**Process:**

- Update development docs during development
- User docs updated before release (coordinate with dbt Labs)
- Changelog entries added after PR creation

## Hatch Commands Reference

```bash
# Environment management
hatch env create                  # Create default environment
hatch env remove                  # Remove environment
hatch shell                       # Enter environment shell

# Code quality
hatch run code-quality            # All checks (format, lint, type-check)
hatch run ruff format             # Format code only
hatch run ruff check              # Lint code only
hatch run mypy                    # Type checking only

# Testing
hatch run unit                    # Unit tests (Python 3.9)
hatch run test:unit               # Unit tests (all Python versions)
hatch run cluster-e2e             # Functional tests (HMS cluster)
hatch run uc-cluster-e2e          # Functional tests (Unity Catalog)
hatch run sqlw-e2e                # Functional tests (SQL Warehouse)

# Building
hatch build                       # Build wheel and sdist
hatch version                     # Show current version
```

### Troubleshooting

If Hatch isn't respecting changes to `pyproject.toml`:

```bash
hatch env prune                   # Remove all environments
hatch env create                  # Recreate default environment
```

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/databricks/dbt-databricks/issues)
- **Contributing**: See [CONTRIBUTING.MD](../CONTRIBUTING.MD) for guidelines
