{% macro union_different_relations(relations, model_columns, incremental_clause=None) %}
	{% set queries = [] %}
	{% for relation in relations %}
		-- get columns from each relation
		{%- set relation_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list | sort -%}

		-- fill in nulls
		{% set select_columns = [] %}
		{% for column in model_columns %}
			{%- if column[0] not in relation_columns -%}
				{% do select_columns.append("CAST(NULL AS " ~ column[1] ~ ") AS " ~ column[0]) %}
			{% else %}
				{% do select_columns.append(column[0]) %}
			{% endif %}
		{% endfor %}

		-- create select statement
		{% set query %}
			SELECT 
				{{ select_columns | join(',\n\t') }} 
			FROM {{ relation }}
			{% if incremental_clause is not none and is_incremental()%}
				WHERE {{ incremental_clause }}
			{% endif %}
		{% endset %}

		{% do queries.append(query) %}
	{% endfor %}

    {{ return(queries | join('\nUNION ALL\n')) }}
{% endmacro %}
