{% macro get_max_partition(partition_field, where_clause=None, lookback_window=None) %}
    -- Construct the SQL query to get the MAX value of the partition field
    {%- set query %}
        SELECT 
            {% if lookback_window %}
                DATE_SUB(MAX({{ partition_field }}), INTERVAL {{ lookback_window}} DAY) AS max_partition
            {% else %}
                MAX({{ partition_field }}) AS max_partition
            {% endif %}
        FROM {{ this }}
        {% if where_clause %}
            WHERE {{ where_clause }}  -- Add the WHERE clause if provided
        {% endif %}
    {% endset %}

    --Execute the SQL query using dbt's run_query function
    {% set result = run_query(query) %}
    
    -- Check if the query returned a result
    {% if result %}
        -- Extract the max value from the first column of the first row in the result set
        {% set max_value = result.columns[0].values()[0] %}
		{% do log('Last partition: ' ~ max_value, info=True) %}
		{{ return(max_value) }}  -- Return the max value as a string
    {% endif %}
{% endmacro %}
