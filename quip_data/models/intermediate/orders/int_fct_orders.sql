/*
This model includes all orders. It combines orders from the legacy system and the shopify system.
*/
WITH
shopify_orders AS (
	SELECT * FROM {{ ref("stg_shopify__orders") }}
)

, shopify_fulfillment_events AS (
	SELECT * FROM {{ ref("stg_shopify__fulfillment_events") }}
)

, shopify_fulfillments AS (
	SELECT * FROM {{ ref("stg_shopify__fulfillments") }}
)

, middleware_fulfillments AS (
	SELECT * FROM {{ ref("stg_quip_public__fulfillment_requests") }}
)

, legacy_orders AS (
	SELECT * FROM {{ ref("stg_quip_public__orders") }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	legacy_order_id AS order_id
	, legacy_subscription_id AS subscription_id

	, created_at
	, updated_at
	, cancelled_at

	-- fulfillment
	, NULL AS fulfillment_provider
	, fulfillment_service_level
	, fulfillment_status
	, NULL AS fulfillment_ship_date
	--, NULL AS fulfillment_delivered_at
	, NULL AS fulfilment_received_by_provider_at

	, order_type
	, 'quip' AS order_source
	, detailed_status
	, order_status

	, weight_in_lbs

	-- payments
	, payment_status
	--, total_cost
	, tax AS total_tax_at_checkout
	, subtotal AS subtotal_price_at_checkout

FROM legacy_orders

UNION ALL

SELECT
	orders.shopify_order_id AS order_id
	, NULL AS subscription_id -- need to bring in recharge orders to get subscription_id

	, orders.created_at
	, orders.updated_at
	, orders.cancelled_at

	-- fulfillment
	, middleware_fulfillments.fulfillment_provider
	, middleware_fulfillments.fulfillment_service_level
	, orders.fulfillment_status
	, middleware_fulfillments.fulfillment_ship_date
	--delivered_at
	, middleware_fulfillments.fulfilment_received_by_provider_at

	, NULL AS order_type
	, 'shopify' AS order_source
	, NULL AS detailed_status
	,CASE 
        WHEN orders.cancelled_at IS NOT NULL THEN 'failed_or_canceled'
        WHEN (orders.fulfillment_status IS NULL OR orders.fulfillment_status = 'partial')
            OR (orders.fulfillment_status = 'fulfilled' AND delivered.event_at IS NULL)
            THEN 'pending'
        WHEN delivered.event_at IS NOT NULL THEN 'delivered'
        ELSE 'no_mapped_order_status_need_to_fix'
        END AS order_status

	, NULL AS weight_in_lbs

	-- payments
	, orders.payment_status
	--, total_cost
	, orders.total_tax_at_checkout
	, orders.subtotal_price_at_checkout

FROM shopify_orders AS orders
LEFT JOIN shopify_fulfillments
	ON shopify_fulfillments.shopify_order_id = orders.shopify_order_id
LEFT JOIN middleware_fulfillments
	ON middleware_fulfillments.shopify_fulfillment_id = shopify_fulfillments.shopify_fulfillment_id
LEFT JOIN shopify_fulfillment_events AS delivered
	ON delivered.shopify_order_id = orders.shopify_order_id
	AND delivered.order_status = 'delivered'
WHERE NOT orders.is_source_deleted