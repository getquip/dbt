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
	SELECT * FROM {{ source('rudderstack_prod', 'order_cancelled') }} 
)

, cleaned AS (
	SELECT
		id AS event_id
		, `timestamp` AS event_at
		, user_id
		, anonymous_id
		, products
		, cancel_reason
		, total_tax
		, checkout_id
		, discount_codes
		, subtotal_price
		, processed_at AS cancellation_processed_at
		, cancelled_at
		, total_price
		, order_number
		, financial_status
		, refunds
		, `event` AS event_name
		, received_at
	FROM source
)

SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1