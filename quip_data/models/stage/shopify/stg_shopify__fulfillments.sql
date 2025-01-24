WITH source AS (
    SELECT * FROM {{ source('shopify', 'fulfillment') }}
)

, renamed AS (
    SELECT
        id AS shopify_fulfillment_id
        , _fivetran_synced AS source_synced_at
        , created_at
        , updated_at
        , order_id AS shopify_order_id
        , service AS fulfillment_service
    FROM source
)

SELECT * FROM renamed
