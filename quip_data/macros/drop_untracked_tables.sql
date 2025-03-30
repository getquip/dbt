{% macro drop_untracked_tables(dry_run=True) %}
    {% set databases = ['quip-dw-stage', 'quip-dw-intermediate', 'quip-dw-mart'] %}

    {% for database in databases %}
        {{ log("Checking " ~ database  ~ " Database...", info=True) }}
        -- Get all schemas from the target database
        {% set db_schemas_query %}
            SELECT DISTINCT schema_name
            FROM {{ database }}.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
        {% endset %}

        {% set db_schemas = dbt_utils.get_query_results_as_dict(db_schemas_query) %}

        -- Get all tables managed by dbt as a dictionary
        {% set tracked_tables = {} %}
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == "model" %}
                {% set schema_name = node.config.schema %}
                {% set table_name = node.alias or node.name %}

                {% if schema_name not in tracked_tables %}
                    {% do tracked_tables.update({schema_name: []}) %}
                {% endif %}
                {% do tracked_tables[schema_name].append(table_name) %}
            {% endif %}
        {% endfor %}

        -- Loop through schemas and check against the tracked dictionary
        {% for schema in db_schemas['schema_name'] %}

			{{ log("Checking: " ~ schema, info=True) }}
            -- Case 1: Schema is NOT tracked (drop entire schema)
            {% if schema not in tracked_tables and schema != 'seeds'%}
                {% if dry_run %}
                    {{ log("Dry Run - Would drop schema: " ~ schema, info=True) }}
                {% else %}
                    {% set drop_schema_query %}
                        DROP SCHEMA IF EXISTS `{{ database }}.{{ schema }}` CASCADE;
                    {% endset %}
                    {% do run_query(drop_schema_query) %}
                    {{ log("Dropped schema: " ~ schema, info=True) }}
                {% endif %}

            -- Case 2: Schema is tracked, check tables
            {% else %}
				{{ log("Checking tables... ", info=True) }}
                -- Get tables from the database for the current schema
                {% set schema_tables_query %}
                    SELECT table_name
                    FROM {{ database }}.{{ schema }}.INFORMATION_SCHEMA.TABLES
                {% endset %}

				{% set db_tables = dbt_utils.get_query_results_as_dict(schema_tables_query) %}

                -- Loop through db tables and drop untracked ones
                {% for table_name in db_tables['table_name'] %}
                    {% if table_name not in tracked_tables[schema] %}
                        {% if dry_run %}
                            {{ log("Dry Run - Would drop table: " ~ schema ~ "." ~ table_name, info=True) }}
                        {% else %}
                            {% set drop_table_query %}
                                DROP TABLE IF EXISTS `{{ database }}.{{ schema }}.{{ table_name }}`
                            {% endset %}
                            {% do run_query(drop_table_query) %}
                            {{ log("Dropped table: " ~ schema ~ "." ~ table_name, info=True) }}
                        {% endif %}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endmacro %}
