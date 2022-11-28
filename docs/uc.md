# Using dbt-databricks with Unity Catalog

[Unity Catalog](https://www.databricks.com/product/unity-catalog) provides a SQL interface to manage tables, views and more across Databricks workspaces using standard SQL. This page describes how to use dbt-databricks with Unity Catalog.

# Benefits of Unity Catalog for analytics engineers

We strongly recommend using Unity Catalog for all dbt projects on Databricks. Unity Catalog brings two important benefits to analytics engineers:

1. Unity Catalog offers a three-level namespace: catalog > schema > table/view. Note that schema is a synonym for database in earlier releases of dbt-databricks. This lets you manage and isolate data and avoids polluting a single database with unrelated tables and views. For example, you can isolate data between your finance and HR teams inside a production catalog called `prod` in two schemas, `finance` and `hr`:

```
catalog: prod
    │
    │schema: finance
    ├──────┐
    │      ├───── revenue
    │      └───── chargebacks
    │
    │schema: hr
    └──────┐
           ├───── people
           └───── salaries
```

2. All privilege management is through standard SQL, which means you don't have to learn a new method of managing access.
3. Unity Catalog offers [automatic column-level lineage](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html).

# Default catalog
When you run `dbt init`, the CLI will ask you for a catalog. This is the _default_ catalog. Note that dbt models can refer to tables and views in any catalog (as long as you have appropriate privileges). The default catalog is simply where dbt will create tables and views by default. Note that [certain limitations](https://docs.databricks.com/data-governance/unity-catalog/hive-metastore.html#joins-between-unity-catalog-and-hive-metastore-objects) apply to joins between tables and views in Unity Catalog and those in Hive metastore.

**Important**: do NOT specify catalog in the `schema` keyword as a two-level object e.g. `my_catalog.my_schema`. Instead, specify it in the `catalog` keyword.


# Defining a catalog for a source

dbt [sources](https://docs.getdbt.com/docs/build/sources) can also be in a Unity Catalog. Specify the catalog like below:

```
version: 2

sources:
  - name: jaffle_shop
    catalog: ecommerce
    schema: product
    tables:
      - name: items
      - name: users
```

# Grant

You can manage grants on tables and views in Unity Catalog using dbt [grants](https://docs.getdbt.com/reference/resource-configs/grants). For example, this is how you grant `SELECT` access on the `cost` model to the `data-engineers` group. Please read about Unity Catalog's [inheritance model](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html#inheritance-model).

```
models:
  - name: cost
    config:
      grants:
        select: ['data-engineers']
```
