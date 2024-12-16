WITH

shopify_customers AS (
    SELECT * FROM {{ ref('stg_shopify__customers') }}
)

, shopify_customer_tag AS (
    SELECT * FROM {{ ref('stg_shopify__customer_tags') }}
)

, legacy_users AS (
    SELECT * FROM {{ ref('stg_quip_public__users') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT -- shopify
    customers.shopify_user_id
    , customers.legacy_quip_user_id

    -- for users pre-shopify, `created_at` represents the migration to shopify
    , IF(customers.legacy_quip_user_id IS NULL, customers.created_at, legacy_users.created_at) AS created_at
    , IF(customers.legacy_quip_user_id IS NOT NULL, customers.created_at, NULL) AS migrated_to_shopify_at
    , customers.updated_at
    
    , customers.email
    , customers.email_hashed
    , customers.phone
    , customers.phone_hashed
    , customers.first_name
    , customers.last_name

    , customers.email_marketing_consent_opt_in_level
    , customers.email_marketing_consent_state

    -- bools
    , tags.shopify_user_id IS NOT NULL AS is_suspected_reseller
    , customers.is_tax_exempt
    , customers.is_verified_email
FROM shopify_customers AS customers
LEFT JOIN shopify_customer_tag AS tags
    ON customers.shopify_user_id = tags.shopify_user_id
    AND tags.tag = 'suspected_reseller'
LEFT JOIN legacy_users
    ON customers.legacy_quip_user_id = legacy_users.legacy_quip_user_id


UNION ALL 

SELECT -- legacy quip *should only union on full refresh
    NULL AS shopify_user_id
    , legacy_users.legacy_quip_user_id

    , legacy_users.created_at
    , NULL AS migrated_to_shopify_at
    , legacy_users.updated_at

    , legacy_users.email
    , NULL AS email_hashed
    , legacy_users.phone
    , NULL AS phone_hashed
    , legacy_users.first_name
    , legacy_users.last_name

    , NULL AS email_marketing_consent_opt_in_level
    , NULL AS email_marketing_consent_state

    -- bools
    , FALSE AS is_suspected_reseller
    , FALSE AS is_tax_exempt
    , is_verified AS is_verified_email
FROM legacy_users
WHERE legacy_quip_user_id NOT IN (SELECT legacy_quip_user_id FROM shopify_customers)