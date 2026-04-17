<p align="center">
  <img src="https://bynder-public-us-west-2.s3.amazonaws.com/styleguide/ABB317701CA31CB7F29268E32B303CAE-pdf-column-1.png" alt="databricks logo" width="50%" />
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

The **[Databricks Lakehouse](https://www.databricks.com/)** provides one simple platform to unify all your data, analytics and AI workloads.

# dbt-databricks-enhanced

An enhanced fork of [`dbt-databricks`](https://github.com/databricks/dbt-databricks) that extends the official adapter with additional capabilities. This is not a copy — it tracks upstream releases and adds features on top.

## What's Different

### Session Mode

The primary enhancement is **session mode** — the ability to run dbt entirely within a SparkSession on Databricks **job clusters**, without requiring the Databricks SQL connector (DBSQL) or all-purpose clusters. This enables significant cost savings for dbt workloads by using the most economical Databricks compute option.

Session mode supports:
- SQL and Python models executing within a single SparkSession
- Seed loading with automatic parameter binding rendering
- Auto-detection of session mode environment with manual override

All features of the original `dbt-databricks` adapter are preserved.

## Versioning

This fork tracks upstream `dbt-databricks` version numbers. When upstream releases `1.11.7`, this fork rebases and releases as `1.11.7`. If fork-specific bugfixes are needed between upstream releases, a fourth version segment is added: `1.11.7.1`, `1.11.7.2`, etc. All versions follow [PEP 440](https://peps.python.org/pep-0440/).

## Upstream Features

All features from the original `dbt-databricks` adapter are included:

- **Easy setup**. No need to install an ODBC driver as the adapter uses pure Python APIs.
- **Open by default**. For example, it uses the open and performant [Delta](https://delta.io/) table format by default. This has many benefits, including letting you use `MERGE` as the default incremental materialization strategy.
- **Support for Unity Catalog**. dbt-databricks supports the 3-level namespace of Unity Catalog (catalog / schema / relations) so you can organize and secure your data the way you like.
- **Performance**. The adapter generates SQL expressions that are automatically accelerated by the native, vectorized [Photon](https://databricks.com/product/photon) execution engine.

## Getting started

### Installation

Install using pip:

```nofmt
pip install dbt-databricks-enhanced
```

Upgrade to the latest version

```nofmt
pip install --upgrade dbt-databricks-enhanced
```

### Profile Setup

```nofmt
your_profile_name:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: [optional catalog name, if you are using Unity Catalog]
      schema: [database/schema name]
      host: [your.databrickshost.com]
      http_path: [/sql/your/http/path]
      token: [dapiXXXXXXXXXXXXXXXXXXXXXXX]
```

### Documentation

For comprehensive documentation on Databricks-specific features, configurations, and capabilities:

- **[Databricks configurations](https://docs.getdbt.com/reference/resource-configs/databricks-configs)** - Complete reference for all Databricks-specific model configurations, materializations, and incremental strategies
- **[Connect to Databricks](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)** - Setup and authentication guide

### Quick Starts

These following quick starts will get you up and running with the `dbt-databricks` adapter:

- [Set up your dbt project with Databricks](https://docs.getdbt.com/guides/set-up-your-databricks-dbt-project)
- Using dbt Cloud with Databricks ([Azure](https://docs.microsoft.com/en-us/azure/databricks/integrations/prep/dbt-cloud) | [AWS](https://docs.databricks.com/integrations/prep/dbt-cloud.html))
- [Running dbt production jobs on Databricks Workflows](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-workflows.md)
- [Using Unity Catalog with dbt-databricks](https://github.com/databricks/dbt-databricks/blob/main/docs/uc.md)
- [Continuous integration in dbt](https://docs.getdbt.com/docs/deploy/continuous-integration)
- [Loading data from S3 into Delta using the databricks_copy_into macro](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-copy-into-macro-aws.md)
- [Contribute to this repository](CONTRIBUTING.MD)

### Compatibility

The `dbt-databricks-enhanced` adapter has been tested:

- with Python 3.10 or above.
- against `Databricks SQL` and `Databricks runtime releases 9.1 LTS` and later.

### Tips and Tricks

## Choosing compute for a Python model

You can override the compute used for a specific Python model by setting the `http_path` property in model configuration. This can be useful if, for example, you want to run a Python model on an All Purpose cluster, while running SQL models on a SQL Warehouse. Note that this capability is only available for Python models.

```
def model(dbt, session):
    dbt.config(
      http_path="sql/protocolv1/..."
    )
```

## Python models and ANSI mode

When ANSI mode is enabled (`spark.sql.ansi.enabled=true`), there are limitations when using pandas DataFrames in Python models:

1. **Regular pandas DataFrames**: dbt-databricks will automatically handle conversion even when ANSI mode is enabled, falling back to `spark.createDataFrame()` if needed.

2. **pandas-on-Spark DataFrames**: If you create pandas-on-Spark DataFrames directly in your model (using `pyspark.pandas` or `databricks.koalas`), you may encounter errors with ANSI mode enabled. In this case, you have two options:
   - Disable ANSI mode for your session: Set `spark.sql.ansi.enabled=false` in your cluster or SQL warehouse configuration
   - Set the pandas-on-Spark option in your model code:
     ```python
     import pyspark.pandas as ps
     ps.set_option('compute.fail_on_ansi_mode', False)
     ```
     Note: This may cause unexpected behavior as pandas-on-Spark follows pandas semantics (returning null/NaN for invalid operations) rather than ANSI SQL semantics (raising errors).

For more information about ANSI mode and its implications, see the [Spark documentation on ANSI compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html).
