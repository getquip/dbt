WITH

subscriptions AS (
	SELECT * FROM {{ ref("stg_recharge__subscriptions") }}
)

, legacy_subscriptions AS (
	SELECT * FROM {{ ref("stg_quip_public__subscriptions") }}
)

, customers AS (
	SELECT * FROM {{ ref("stg_recharge__customers") }}
)

, orders AS (
	SELECT * FROM {{ ref("stg_shopify__orders") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, latest_orders AS (
	SELECT
		orders.shopify_customer_id
		, shopify_order_id AS latest_shopify_order_id
	FROM orders
	QUALIFY ROW_NUMBER() OVER 
		(PARTITION BY orders.shopify_customer_id ORDER BY orders.updated_at DESC) = 1
)

-- need period start and end dates. this is based on the latest orders charge created and charge scheduled date

SELECT
	-- ids
	subscriptions.subscription_id
	, subscriptions.recharge_customer_id
	, customers.shopify_customer_id
	, subscriptions.address_id
	-- this might need to be replaced with FIRST order_id
	, COALESCE(legacy_subscriptions.latest_order_id, latest_orders.latest_shopify_order_id) AS latest_order_id
	
	-- cancellations
	, COALESCE(legacy_subscriptions.created_at, subscriptions.created_at) AS created_at
	, subscriptions.cancelled_at
	
	, subscriptions.status
	, subscriptions.quantity

FROM subscriptions
LEFT JOIN legacy_subscriptions
	ON subscriptions.legacy_quip_subscription_id = legacy_subscriptions.legacy_quip_subscription_id
LEFT JOIN customers
	ON subscriptions.recharge_customer_id = customers.recharge_customer_id
LEFT JOIN latest_orders
	ON customers.shopify_customer_id = latest_orders.shopify_customer_id

