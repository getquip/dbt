WITH

items AS (
	SELECT * FROM {{ ref('stg_shopify__order_lines') }}
)

, subscriptions AS (
	SELECT * FROM {{ ref('stg_recharge__subscriptions') }}
)

, orders AS (
	SELECT * FROM {{ ref('stg_shopify__orders') }}
)

, recharge_items AS (
	SELECT * FROM {{ ref('stg_recharge__line_items') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, subscription_items AS (
	SELECT
		line_item_id
		, subscription_id
		, shopify_order_id
		, shopify_product_id
		, shopify_product_variant_id
		, sku
		, unit_price
		, quantity
		, is_taxable
		, total_discount
	FROM recharge_items
)

SELECT
	CAST(shopify_line_item_id AS STRING) AS line_item_id
	, CAST(NULL AS INTEGER) AS subscription_id
	, shopify_order_id
	, shopify_product_id
	, shopify_product_variant_id
	, sku
	, unit_price
	, quantity
	, is_taxable
	, total_discount
FROM items
WHERE shopify_order_id NOT IN (SELECT shopify_order_id FROM subscription_items)

UNION ALL

SELECT * FROM subscription_items
