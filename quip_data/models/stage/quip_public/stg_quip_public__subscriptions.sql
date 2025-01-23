WITH source AS (
    SELECT * FROM {{ source('quip_public', 'subscriptions') }}
)

, renamed AS (
    SELECT
        _fivetran_deleted AS is_source_deleted
        , _fivetran_synced AS source_synced_at

        , id AS legacy_subscription_id
        , user_id AS legacy_customer_id
        , address_id
        , order_id AS first_order_id -- this is the order that created the subscription
        , status
        , quantity
        , SAFE_CAST(created_at AS TIMESTAMP) AS created_at
        , SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
        , SAFE_CAST(canceled_at AS TIMESTAMP) AS cancelled_at
    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted 
