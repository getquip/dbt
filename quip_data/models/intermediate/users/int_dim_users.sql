{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=["is_suspected_reseller", "email_marketing_consent_opt_in_level", "legacy_customer_id", "shopify_customer_id"]
) }}

WITH

shopify_customers AS (
    SELECT * FROM {{ ref('stg_shopify__customers') }}
)

, shopify_customer_tag AS (
    SELECT * FROM {{ ref('stg_shopify__customer_tags') }}
)

, recharge_customers AS (
    SELECT * FROM {{ ref('stg_recharge__customers') }}
)

, legacy_users AS (
    SELECT * FROM {{ ref('stg_quip_public__users') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT -- shopify
    shopify_customer_id
    , recharge_customers.id AS recharge_customer_id
    , legacy_customer_id

    -- for users pre-shopify, `created_at` represents the migration to shopify
    , IF(legacy_customer_id IS NULL , created_at , legacy_users.created_at) AS created_at
    , IF(legacy_customer_id IS NULL , NULL , created_at) AS migrated_to_shopify_at
    , updated_at

    , email
    , email_hashed
    , phone
    , phone_hashed

    , email_marketing_consent_opt_in_level
    , email_marketing_consent_state

    -- bools
    , tags.shopify_customer_id IS NOT NULL AS is_suspected_reseller
    , is_tax_exempt
    , is_verified_email
FROM shopify_customers
LEFT JOIN shopify_customer_tag AS tags
    ON shopify_customer_id = tags.shopify_customer_id
        AND tags.tag = 'suspected_reseller'
LEFT JOIN recharge_customers
    ON recharge_customers.external_customer_id_ecommerce = shopify_customers.shopify_customer_id
LEFT JOIN legacy_users
    ON legacy_customer_id = legacy_users.legacy_customer_id


UNION ALL

SELECT -- legacy quip *should only union on full refresh
    NULL AS shopify_customer_id
    , NULL AS recharge_customer_id
    , legacy_users.legacy_customer_id

    , legacy_users.created_at
    , NULL AS migrated_to_shopify_at
    , legacy_users.updated_at

    , legacy_users.email
    , email_hashed
    , legacy_users.phone
    , phone_hashed

    , NULL AS email_marketing_consent_opt_in_level
    , NULL AS email_marketing_consent_state

    -- bools
    , FALSE AS is_suspected_reseller
    , FALSE AS is_tax_exempt
    , FALSE AS is_verified_email
FROM legacy_users
WHERE legacy_customer_id NOT IN (SELECT legacy_customer_id FROM shopify_customers)
