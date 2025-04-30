{% test assert_breakout_equals_total(model, column_name, parent_table, parent_column, join_cols) %}

WITH breakout AS (
    SELECT
        {% for col in join_cols %}
            {{ col }},
        {% endfor %}
        SUM({{ column_name }}) AS breakout_sum
    FROM {{ model }}
    GROUP BY {{ join_cols | join(", ") }}
),

parent AS (
    SELECT
        {% for col in join_cols %}
            {{ col }},
        {% endfor %}
        {{ parent_column }} AS parent_value
    FROM {{ parent_table }}
),

comparison AS (
    SELECT
        breakout.*,
        parent.parent_value
    FROM breakout AS breakout
    INNER JOIN parent AS parent
        ON {% for col in join_cols %}
            breakout.{{ col }} = parent.{{ col }}{% if not loop.last %} AND {% endif %}
        {% endfor %}
)

SELECT 
	*
	, ABS(breakout_sum - parent_value) AS diff
FROM comparison
WHERE ROUND(ABS(breakout_sum - parent_value), 2) > 0.1

{% endtest %}
