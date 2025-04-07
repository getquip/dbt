{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "event_name", 
        "anonymous_id",
        "event_id"
    ]
) }}


{% set relations = [
    ref('base_customer_data_platform__legacy_event_context') 
    , ref('stg_rudderstack__product_added') 
    , ref('stg_rudderstack__order_created') 
    , ref('stg_rudderstack__order_cancelled') 
] %}

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
 WITH

 joined AS (
    {% set queries = [] %}

    {% set model_columns = [
        ('event_id', 'STRING')
        , ('user_id', 'STRING')
        , ('anonymous_id', 'STRING')
        , ('event_at', 'TIMESTAMP')
        , ('event_name', 'STRING')
        , ('removed_cart_item_quantity', 'INTEGER')
        , ('removed_cart_item_value', 'NUMERIC')
        , ('removed_cart_item', 'STRING')
        , ('product_added_name', 'STRING')
        , ('subscription_id', 'INTEGER')
        , ('total_subscription_cost', 'NUMERIC')
        , ('source_name', 'STRING')
        , ('sku', 'STRING')
        , ('target_location', 'STRING')
        , ('target_text', 'STRING')
        , ('target_type', 'STRING')
        , ('target_name', 'STRING')
        , ('target_ref', 'STRING')
        , ('new_refill_date', 'DATE')
        , ('previous_refill_date', 'DATE')
        , ('scrolled_page_percentage', 'NUMERIC')
    ] %}

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
        {% endset %}

        {% do queries.append(query) %}
    {% endfor %}

    {{ queries | join('\nUNION ALL\n') }}
 )

{% set customer_actions = [
	'product_added'
	, 'order_created'
	, 'order_cancelled'
    , 'order_completed'
    , 'subscription_canceled'
    , 'coupon_applied'
    , 'checkout_step_viewed'
    , 'checkout_step_completed'
    , 'checkout_started'
	
] %}

SELECT
    joined.*
    , CASE WHEN event_name IN ({{ "'" ~ customer_actions | join("', '") ~ "'" }})
        THEN 'customer_actions'
        ELSE 'automated_processes'
    END AS event_type
FROM joined

    