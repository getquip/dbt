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
        "user_id", 
        "anonymous_id",
        "event_id"
    ]
) }}

WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'product_added') }} 
)

, cleaned AS (
	SELECT
		id AS event_id
		, `timestamp` AS event_at
		, user_id
		, COALESCE(anonymous_id, user_id) AS anonymous_id
		, received_at
		, product_id
		, price
		, sku AS sku_presentment
        , REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, variant
		, `event` AS event_name
	FROM source
)

SELECT * FROM cleaned
WHERE received_at >= '2025-04-01'
{% if is_incremental() %}
	AND received_at >= "{{ get_max_partition('received_at') }}"
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1