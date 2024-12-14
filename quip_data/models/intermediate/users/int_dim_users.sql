WITH

shopify_customers AS (
    SELECT * FROM {{ ref('stg_shopify__customers') }}
)

, shopify_customer_tag AS (
    SELECT * FROM {{ ref('stg_shopify__customer_tags') }}
)


SELECT
    user_id
    , created_at -- for users pre-shopify, this represents the migration to shopify date
    , updated_at
    
    , email
    , email_hashed
    , phone
    , phone_hashed
    , first_name
    , last_name

    , division --
    , email_consent_opt_in_level
    , email_marketing_consent_state

    -- bools
    , tags.user_id IS NOT NULL AS is_suspected_reseller
    , is_tax_exempt
    , is_verified_email
FROM shopify_customers AS customers
LEFT JOIN shopify_customer_tag AS tags
    ON customers.user_id = tags.user_id
    AND tags.tag = 'suspected_reseller'
WHERE NOT is_source_deleted