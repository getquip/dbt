WITH source AS (
    SELECT * FROM {{ source('recharge', 'customers') }}
)

SELECT
    id AS recharge_customer_id
    , SAFE_CAST(external_customer_id.ecommerce AS INTEGER) AS shopify_customer_id
FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC
    ) = 1
