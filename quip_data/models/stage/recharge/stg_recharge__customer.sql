WITH source AS (
    SELECT * FROM {{ source('recharge', 'customer') }}
)

, renamed AS (
    SELECT
        id AS shopify_customer_id -- is this correct?
        , _fivetran_deleted AS is_source_deleted
        , _fivetran_synced AS source_synced_at
        , created_at
        , email
        , external_customer_id_ecommerce AS user_id -- is this shopify or quip?
        , first_charge_processed_at
        , first_name
        , has_payment_method_in_dunning
        , has_valid_payment_method
        , `hash`
        , is_deleted
        , last_name
        , subscriptions_active_count
        , subscriptions_total_count
        , tax_exempt
        , updated_at
    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted