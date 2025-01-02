WITH source AS (
	SELECT * FROM {{ source('recharge', 'subscriptions') }}
)

SELECT
	id AS subscription_id

	, created_at
	, updated_at
	, source_synced_at
	, status

	-- customer related
	, customer_id AS recharge_customer_id
	, address_id

	-- product
	, SAFE_CAST(external_product_id.ecommerce AS INTEGER) AS shopify_product_id
	, SAFE_CAST(external_variant_id.ecommerce AS INTEGER) AS shopify_product_variant_id
	, LOWER(product_title) AS product_title
	, LOWER(variant_title) AS variant_title
	, quantity

	-- cancellations
	, cancelled_at
	, cancellation_reason
	, cancellation_reason_comments

	-- nested fields
	, SAFE_CAST(
		IF(property.name = 'legacy_subscriptions_id', property.value, NULL)
		AS INTEGER
		) AS legacy_quip_subscription_id

FROM source
, UNNEST(properties) AS property
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1


