{% macro upload_file(local_file_path, dbfs_file_path, overwrite, contents, headers) %}
  {{ return(adapter.dispatch('upload_file', 'dbt')(local_file_path, dbfs_file_path, overwrite, contents, headers)) }}
{% endmacro %}