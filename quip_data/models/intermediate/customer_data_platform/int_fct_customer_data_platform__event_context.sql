{{ config(
    materialized='incremental',
	incremental_strategy='merge',
	unique_key='event_id',
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
    ref('stg_rudderstack__product_added') 
    , ref('stg_rudderstack__order_created') 
    , ref('stg_rudderstack__order_cancelled') 
] %}

{% if not is_incremental() %}
    {% do relations.append(ref('base_customer_data_platform__legacy_event_context')) %}    
    {% set incremental_clause = None %}
{% else %}
    {% set incremental_clause = "event_at >= '" ~ get_max_partition('event_at', lookback_window=30) ~ "'" %}
{% endif %}

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
 WITH

 joined AS (

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

    {{ union_different_relations(relations, model_columns, incremental_clause) }}

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

    