
{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "status", 
		"recharge_customer_id",
		"shopify_product_id",
        "subscription_id",
    ]
)}}

WITH source AS (
    SELECT * FROM {{ source('recharge', 'subscriptions') }}
)

, renamed AS (
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
		, properties AS legacy_subscription_properties
	FROM source
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
    , quantity
    , cancelled_at
    , cancellation_reason
    , cancellation_reason_comments
    , SAFE_CAST(external_product_id.ecommerce AS INTEGER) AS shopify_product_id

    -- cancellations
    , SAFE_CAST(external_variant_id.ecommerce AS INTEGER) AS shopify_product_variant_id
    , LOWER(product_title) AS product_title
    , LOWER(variant_title) AS variant_title

    -- nested fields
    , SAFE_CAST(
        IF(property.name = 'legacy_subscriptions_id' , property.value , NULL)
        AS INTEGER
    ) AS legacy_subscription_id

FROM source
LEFT JOIN UNNEST(properties) AS property
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC
    ) = 1
