<p align="center">
  <img src="https://bynder-public-us-west-2.s3.amazonaws.com/styleguide/ABB317701CA31CB7F29268E32B303CAE-pdf-column-1.png" alt="databricks logo" width="50%" />
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>
<p align="center">
  <a href="https://github.com/databricks/dbt-databricks/actions/workflows/main.yml">
    <img src="https://github.com/databricks/dbt-databricks/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
  <a href="https://github.com/databricks/dbt-databricks/actions/workflows/integration.yml">
    <img src="https://github.com/databricks/dbt-databricks/actions/workflows/integration.yml/badge.svg?event=push" alt="Integration Tests Badge"/>
  </a>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

The **[Databricks Lakehouse](https://www.databricks.com/)** provides one simple platform to unify all your data, analytics and AI workloads.

# dbt-databricks

The `dbt-databricks` adapter contains all of the code enabling dbt to work with Databricks.

This adapter is based off the amazing work done in [dbt-spark](https://github.com/dbt-labs/dbt-spark)

## Getting started

### Installation

Install using pip:
```nofmt
pip install dbt-databricks
```

Upgrade to the latest version
```nofmt
pip install --upgrade dbt-databricks
```

### Profile Setup

```nofmt
your_profile_name:
  target: dev
  outputs:
    dev:
      type: databricks
      schema: [database/schema name]
      host: [your.databrickshost.com]
      http_path: [/sql/your/http/path]
      token: [dapiXXXXXXXXXXXXXXXXXXXXXXX]
```

### Compatibility

The `dbt-databricks` adapter has been tested against `Databricks SQL` and `Databricks runtime releases 9.1 LTS` and later.

