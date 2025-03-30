{% macro parse_json_array_to_array(original_array, key_to_parse, cast='STRING') %}
  ARRAY(
    SELECT CAST(JSON_EXTRACT_SCALAR(item, '$.{{ key_to_parse }}') AS {{ cast }})
    FROM UNNEST(JSON_EXTRACT_ARRAY({{ original_array }})) AS item
  )
{% endmacro %}
