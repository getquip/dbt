{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "email_marketing_consent_opt_in_level", 
        "is_verified_email", 
        "is_tax_exempt", 
        "shopify_customer_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer') }}
)

, metafield AS (
    SELECT * FROM {{ ref('stg_shopify__metafields') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, renamed AS (
    SELECT
        -- ids
        id AS shopify_customer_id

        -- timestamps
        , _fivetran_synced AS source_synced_at
        -- convert to UTC timestamp
        , created_at
        , updated_at
        , email_marketing_consent_consent_updated_at

        -- bools
        , verified_email AS is_verified_email
        , tax_exempt AS is_tax_exempt

        -- ints
        , orders_count

        -- floats
        , total_spent AS total_revenue

        -- strings    
        -- hashed PII
        , {{ generate_hashed_pii_fields([
            'email'
            , 'phone'
            ]) }}
        , first_name
        , last_name
        , currency
        , LOWER(email_marketing_consent_opt_in_level) AS email_marketing_consent_opt_in_level
        , LOWER(email_marketing_consent_state) AS email_marketing_consent_state
        , multipass_identifier
        , note
        , LOWER(state) AS state


    FROM source
)

SELECT
    renamed.*
    , SAFE_CAST(metafield.value AS INTEGER) AS legacy_customer_id
FROM renamed
LEFT JOIN metafield
    ON renamed.shopify_customer_id = metafield.resource_id
        AND metafield.resource_table_name = 'customer'
        AND metafield.key = 'user_id'