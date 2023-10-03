

  - applied state stuff?
  - relation specific drop and rename (no defaults)
  - modify materialization logic
  - python class stuff
    - indicate relations that are replaceable vs renamable
    - a
    - lot
    - of things
  - add new MV tests


## questions

### what's the new naming convention?

- `DatabricksRelationConfigBase`

### overly sparse `RelationConfigBase`

why does `RelationConfigBase` (`adapters/relation_configs/config_base.py`) only have two methods? all the other ones are copied and pasted?

RelationConfigBase's methods
      1. `from_dict`
      2. `_not_implemented_error`

all the other adapters
      1. `include_policy`
      2. `quote_policy`
      3. `from_model_node`
      4. `parse_model_node`
      5. `from_relation_results`
      6. `parse_relations_results`
      7. `_render_part`
      8. `_get_first_row`