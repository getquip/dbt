WITH source AS (
    SELECT * FROM {{ source('quip_public', 'fulfillment_requests') }}
)

, renamed AS (
    SELECT
        id AS fulfillment_request_id
        , _fivetran_synced AS source_synced_at
        , created_at
        , updated_at
        , fulfillment_completed_at AS fulfillment_ship_date
        , fulfillment_message
        , fulfillment_order_id
        , fulfillment_provider
        , service_level AS fulfillment_service_level
        , received_by_provider_at AS fulfilment_received_by_provider_at
        , ship_method_code
        , status AS request_status
        , provider_status
        , ship_method
        , COALESCE(_fivetran_deleted , FALSE) AS is_source_deleted
        , SAFE_CAST(shopify_fulfillment_id AS INTEGER) AS shopify_fulfillment_id
    FROM source
)

SELECT *
FROM renamed
WHERE NOT is_source_deleted
