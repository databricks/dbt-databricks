{#- Returns none when no Compatibility Mode location is involved, else {'strategy': 'replace' | 'drop' | 'refuse', 'reason': ...}. -#}
{% macro mv_compatibility_rebuild_strategy(desired_partition_by, existing_partition_by, desired_tblproperties, existing_tblproperties) %}
  {%- set location_key = 'delta.universalFormat.compatibility.location' -%}
  {%- set desired_location = desired_tblproperties.get(location_key) -%}
  {%- set existing_location = existing_tblproperties.get(location_key) -%}
  {%- if desired_location is none and existing_location is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- for side, location in [('desired', desired_location), ('existing', existing_location)] -%}
    {%- if location is not none and location | trim == '' -%}
      {{ return({'strategy': 'refuse', 'reason': "the " ~ side ~ " '" ~ location_key ~ "' is set but empty. Set it to a Unity Catalog location, or remove the property to disable Compatibility Mode. No replacement or drop was attempted."}) }}
    {%- endif -%}
  {%- endfor -%}

  {%- if desired_location and existing_location and desired_location != existing_location -%}
    {{ return({'strategy': 'refuse', 'reason': "changing the Compatibility Mode location from '" ~ existing_location ~ "' to '" ~ desired_location ~ "' can't be done in place, and dbt won't drop the view to move it: the re-create could fail with LOCATION_OVERLAP and leave no view. Drop the view manually and rebuild it at a location that doesn't overlap '" ~ existing_location ~ "'. No replacement or drop was attempted."}) }}
  {%- endif -%}

  {%- set desired_parts = desired_partition_by | map('lower') | map('replace', '`', '') | list -%}
  {%- set existing_parts = existing_partition_by | map('lower') | map('replace', '`', '') | list -%}
  {%- if desired_parts == existing_parts -%}
    {{ return({'strategy': 'replace'}) }}
  {%- elif not desired_location -%}
    {{ return({'strategy': 'drop'}) }}
  {%- endif -%}
  {{ return({'strategy': 'refuse', 'reason': "changing partition_by needs a drop, and dbt won't drop a view whose re-create has a Compatibility Mode location ('" ~ desired_location ~ "'): the create could fail with LOCATION_OVERLAP and leave no view. Remove the Compatibility Mode properties and change partition_by in one run, then add the properties back with a location that doesn't overlap '" ~ desired_location ~ "' (the drop reserves it). No replacement or drop was attempted."}) }}
{% endmacro %}
