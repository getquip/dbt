WITH

orders AS (
	SELECT * FROM {{ ref('stg_rudderstack__order_created') }}
)

, event_sessions AS (
	SELECT * FROM {{ ref('base_customer_data_platform__legacy_sessions') }}
)

, legacy AS (
	SELECT * FROM {{ ref('base_customer_data_platform__legacy_orders') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	source_name
	, session_id
	, event_id
	, shopify_order_id
	, checkout_id
	, event_at
	, discount_code
	, fulfillment_status
	, payment_gateway_names
	, products
	, affiliation
FROM legacy

UNION ALL

SELECT
	source_name
	, session_id
	, event_id
	, shopify_order_id
	, checkout_id
	, event_at
	, discount_codes
	, fulfillment_status
	, payment_gateway_names
	, products
	, NULL AS affiliation
FROM orders