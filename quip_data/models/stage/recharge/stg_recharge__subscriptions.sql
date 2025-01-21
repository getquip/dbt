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
) }}

WITH source AS (
    SELECT * FROM {{ source('recharge', 'subscriptions') }}
)

, renamed AS (
    SELECT
        id AS subscription_id
        , customer_id AS recharge_customer_id
        , created_at
        , updated_at
        , source_synced_at
        , status
        , address_id
        , quantity
        , cancelled_at
        , cancellation_reason
        , cancellation_reason_comments
        , properties AS legacy_subscription_properties
        , SAFE_CAST(external_product_id.ecommerce AS INTEGER) AS shopify_product_id
        , SAFE_CAST(external_variant_id.ecommerce AS INTEGER) AS shopify_product_variant_id
        , LOWER(product_title) AS product_title
        , LOWER(variant_title) AS variant_title
    FROM source
)


SELECT
    renamed.*
    , CAST(property.value AS INTEGER) AS legacy_subscription_id

FROM renamed
LEFT JOIN UNNEST(legacy_subscription_properties) AS property
    ON property.name = 'legacy_subscriptions_id'
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY subscription_id
        ORDER BY updated_at DESC
    ) = 1
