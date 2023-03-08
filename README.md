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

The `dbt-databricks` adapter contains all of the code enabling dbt to work with Databricks. This adapter is based off the amazing work done in [dbt-spark](https://github.com/dbt-labs/dbt-spark). Some key features include:

- **Easy setup**. No need to install an ODBC driver as the adapter uses pure Python APIs.
- **Open by default**. For example, it uses the the open and performant [Delta](https://delta.io/) table format by default. This has many benefits, including letting you use `MERGE` as the the default incremental materialization strategy.
- **Support for Unity Catalog**. dbt-databricks>=1.1.1 supports the 3-level namespace of Unity Catalog (catalog / schema / relations) so you can organize and secure your data the way you like.
- **Performance**. The adapter generates SQL expressions that are automatically accelerated by the native, vectorized [Photon](https://databricks.com/product/photon) execution engine.

## Choosing between dbt-databricks and dbt-spark
If you are developing a dbt project on Databricks, we recommend using `dbt-databricks` for the reasons noted above.

`dbt-spark` is an actively developed adapter which works with Databricks as well as Apache Spark anywhere it is hosted e.g. on AWS EMR.

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
      catalog: [optional catalog name, if you are using Unity Catalog, only available in dbt-databricks>=1.1.1]
      schema: [database/schema name]
      host: [your.databrickshost.com]
      http_path: [/sql/your/http/path]
      token: [dapiXXXXXXXXXXXXXXXXXXXXXXX]
```

### Quick Starts

These following quick starts will get you up and running with the `dbt-databricks` adapter:
- [Developing your first dbt project](https://github.com/databricks/dbt-databricks/blob/main/docs/local-dev.md)
- Using dbt Cloud with Databricks ([Azure](https://docs.microsoft.com/en-us/azure/databricks/integrations/prep/dbt-cloud) | [AWS](https://docs.databricks.com/integrations/prep/dbt-cloud.html))
- [Running dbt production jobs on Databricks Workflows](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-workflows.md)
- [Using Unity Catalog with dbt-databricks](https://github.com/databricks/dbt-databricks/blob/main/docs/uc.md)
- [Using GitHub Actions for dbt CI/CD on Databricks](https://github.com/databricks/dbt-databricks/blob/main/docs/github-actions.md)
- [Loading data from S3 into Delta using the databricks_copy_into macro](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-copy-into-macro-aws.md)
- [Contribute to this repository](CONTRIBUTING.MD)

### Compatibility

The `dbt-databricks` adapter has been tested:

- with Python 3.7 or above.
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
