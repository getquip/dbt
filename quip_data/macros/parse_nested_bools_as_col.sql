{% macro parse_nested_bools_as_col(source_table, nested_column) %}
  -- Step 1: Get the distinct tag values
  {% set possible_values_query %}
    SELECT DISTINCT 
      LOWER(TRIM(item)) AS item
    FROM {{ source_table }},
    UNNEST(SPLIT({{ nested_column }}, ',')) AS item
  {% endset %}
  
  {% set possible_values = run_query(possible_values_query).columns['item'] %}
  
  -- Step 2: Generate SQL columns dynamically for each tag
  {%- for item in possible_values %}
    , '{{ item }}' IN UNNEST({{ nested_column }}) AS is_{{ item | replace(' ', '_') | replace(':', '_') | lower }}
  {%- endfor %}
{% endmacro %}
