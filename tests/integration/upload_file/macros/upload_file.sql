{% macro upload_file(local_file_path, dbfs_file_path, overwrite, contents, headers) %}
  {% do adapter.upload_file(local_file_path, dbfs_file_path, overwrite, contents, headers) %}
{% endmacro %}