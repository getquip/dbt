{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
) }}

{% set model_columns = [
    ('event_id', 'STRING')
    , ('user_id', 'STRING')
    , ('anonymous_id', 'STRING')
    , ('event_at', 'TIMESTAMP')
    , ('event_name', 'STRING')
    , ('target_location', 'STRING')
    , ('target_text', 'STRING')
    , ('target_type', 'STRING')
    , ('target_name', 'STRING')
    , ('target_ref', 'STRING')
    , ('removed_cart_item_quantity', 'INTEGER')
    , ('removed_cart_item_value', 'NUMERIC')
    , ('removed_cart_item', 'STRING')
    , ('product_added_name', 'STRING')
    , ('scrolled_page_percentage', 'NUMERIC')
    , ('subscription_id', 'INTEGER')
    , ('total_subscription_cost', 'NUMERIC')
    , ('new_refill_date', 'DATE')
    , ('previous_refill_date', 'DATE')
    , ('source_name', 'STRING')
] %}



{% set relations = [
    ref('stg_legacy_segment__click') 
    , ref('stg_legacy_segment__clicked') 
    , ref('stg_legacy_segment__clicked_undefined') 
    , ref('stg_legacy_segment__hover') 
    , ref('stg_legacy_segment__mouse_over') 
    , ref('stg_legacy_segment__mixed_cart_modal_removal_accepted') 
    , ref('stg_legacy_segment__product_added') 
    , ref('stg_legacy_segment__scrolled') 
    , ref('stg_legacy_segment__subscription_canceled') 
    , ref('stg_legacy_segment__subscription_next_refill_date_changed') 
    , ref('stg_legacy_segment__tapped') 
    , ref('stg_legacy_segment__order_completed') 
    , ref('stg_littledata__order_completed')
] %}
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
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
    {% endset %}

    {% do queries.append(query) %}
{% endfor %}

{{ queries | join('\nUNION ALL\n') }}
