# dbt-databricks constraints vs DBT 1.5 model contracts 

dbt-databricks constraints are enabled for a model by setting  `persist_constraints: true`  in the model configuration.  Model contracts are enabled by setting `enforced: true` under the contract configuration.

```
models:
  - name: table_model
    config:
      contract:
        enforced: true
```

DBT model contracts enforce column names and datatypes.  This means that **all** columns must be explicitly listed and have name and data_type properties.

dbt-databricks constraints list model level constraints under `meta: constraints:` while in DBT `constraints` is a property of the model.
```
dbt-databricks

models:
  - name: incremental_model
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0
```

```
model contract

models:
  - name: table_model
    constraints:
      - type: check
        columns: [id]
        name: id_greater_than_zero
        expression: "id > 0"
```

dbt-databricks constraints have a single column level constraint (currently limited to not_null) defined by the `meta: constraint:`  property.
Model contracts have multiple column constraints listed under a columns `constraints` property.
```
dbt-databricks

    columns:
      - name: name
        meta:
          constraint: not_null
```

```
model contract

    columns:
      - name: name
        data_type: string
        constraints:
          - type: not_null
```

Model contract constraint structure:
- **type** (required): dbt-databricks constraints do not have this property. DBT has  not_null, check, primary_key, foreign_key, and custom types. dbt-databricks constraints currently support the equivalents of not_null and check.
- **expression**: Free text input to qualify the constraint. In dbt-databricks constraints this is the  condition property. Note: in model contracts the expression text is contained by double quotes, the condition text in dbt-databricks constraints is not double quoted.
- **name** (optional in model contracts, required for check constraints in dbt-databricks constraints): Human-friendly name for this constraint.
- **columns** (model-level only): List of column names to apply the constraint over. dbt-databricks constraints do not have this property.


 In a model contract a check constraint over a single column can be defined at either the model or the column level, but it is recommended that it be defined at the column level. Check constraints over multiple columns must be defined at the model level.
dbt-databricks check constraints are defined only at the model level.

```
dbt-databricks

models:
  - name: my_model
    meta:
      constraints:
        - name: id_greater_than_zero
          condition: id > 0	
    columns:
      - name: name
        meta:
          constraint: not_null
```

```
model contract

models:
  - name: my_model	
    columns:
      - name: name
      data_type: integer
      constraints: 
        - type: not_null
        - type: check
          name: id_greater_than_zero
          expression: "id > 0"
```