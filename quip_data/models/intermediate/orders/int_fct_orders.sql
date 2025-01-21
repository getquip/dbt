/*
This model includes all orders. It combines orders from the legacy system and the shopify system.
*/
WITH
shopify_orders AS (
    SELECT * FROM {{ ref("stg_shopify__orders") }}
)

, metafields AS (
	SELECT
		resource_id AS order_id
		, key
		, LOWER(value) AS value
	FROM {{ ref("stg_shopify__metafields") }}
)

, shopify_fulfillments AS (
    SELECT * 
    FROM {{ ref("stg_shopify__fulfillments") }}
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

, fulfillment_provider AS (
    SELECT
        shopify.shopify_order_id
        , middleware.fulfillment_provider
        , middleware.fulfillment_service_level
        , MIN(middleware.fulfilment_received_by_provider_at) AS fulfilment_received_by_provider_at
        , MIN(middleware.fulfillment_ship_date) AS fulfillment_ship_date
        , MAX(middleware.updated_at) AS updated_at
    FROM shopify_fulfillments AS shopify
    LEFT JOIN middleware_fulfillments AS middleware
        ON shopify.shopify_fulfillment_id = middleware.shopify_fulfillment_id
    GROUP BY 1, 2, 3
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shopify.shopify_order_id 
        ORDER BY updated_at DESC) = 1
)

SELECT
    legacy_order_id AS order_id

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

    , total_weight_lbs

    -- payments
    , payment_status
    --, total_cost
    , tax AS total_tax_at_checkout
    , subtotal AS subtotal_price_at_checkout

FROM legacy_orders

UNION ALL

SELECT
    orders.shopify_order_id AS order_id

    , orders.created_at
    , orders.updated_at
    , orders.cancelled_at

    -- fulfillment
    , fulfillment.fulfillment_provider
    , fulfillment.fulfillment_service_level
    , orders.fulfillment_status
    , fulfillment.fulfillment_ship_date
    --delivered_at
    , fulfillment.fulfilment_received_by_provider_at

    , CASE 
		WHEN orders.source_name = 'subscription_contract' 
			THEN 'subscription-invoice-item'
		WHEN orders.source_name = 'web' THEN 'store'
		WHEN metafields.value = 'replacement' THEN 'replacement'
		WHEN metafields.value = 'reshipment' THEN 'reshipment'
		WHEN metafields.value = 'wholesale'THEN 'wholesale-dental-supplier'
		ELSE 'other'
	END AS order_type
    , 'shopify' AS order_source
    , NULL AS detailed_status
    , CASE
			WHEN orders.cancelled_at IS NOT NULL THEN 'failed_or_canceled'
			WHEN (orders.fulfillment_status IS NULL OR orders.fulfillment_status = 'partial')
				OR (orders.fulfillment_status = 'fulfilled')
				THEN 'pending'
			ELSE 'no_mapped_order_status_need_to_fix'
		END AS order_status
    

    , total_weight_lbs

    -- payments
    , orders.payment_status
    --, total_cost
    , orders.total_tax_at_checkout
    , orders.subtotal_price_at_checkout

FROM shopify_orders AS orders
LEFT JOIN metafields
	ON orders.shopify_order_id = metafields.order_id
	AND metafields.key = 'order_type'
	AND metafields.value IN ('replacement', 'reshipment', 'wholesale')
	AND orders.source_name IN ('1662707', 'shopify_draft_order')
LEFT JOIN fulfillment_provider AS fulfillment
    ON orders.shopify_order_id = fulfillment.shopify_order_id
