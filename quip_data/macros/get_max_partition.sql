{% macro get_max_partition(partition_field, where_clause=None) %}
    {% set query%}
		SELECT MAX({{ partition_field }})
		FROM {{ this }}
		{% if where_clause %}
			WHERE {{ where_clause }}
		{% endif %}
	{% endset %}
	
	{% set result = run_query(sql) %}
	
    {% if result %}
        {% set max_value = result.columns[0].values()[0] %}
        {{ return(max_value) }}
    {% else %}
        {{ return(None) }}
    {% endif %}
{% endmacro %}
