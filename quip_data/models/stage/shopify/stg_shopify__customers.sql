WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer') }}
)

, renamed AS (
    SELECT
        -- ids
        id AS user_id

        -- timestamps
        , _fivetran_synced AS source_synced_at
        , _fivetran_deleted AS is_source_deleted
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
        , metafield			
        , multipass_identifier			
        , note			
        , LOWER(state) AS state			


    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted