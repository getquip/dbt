WITH

shopify_customers AS (
    SELECT * FROM {{ ref('stg_shopify__customers') }}
)


SELECT
    shop.id AS user_id ---?
    , created_at
    , updated_at
    , first_name
    , last_name
    , email
    , phone
    , hashed_phone
    , hashed_email --
    , division --
    , email_consent_opt_in_level
    , email_marketing_consent_state

    , is_suspected_reseller

FROM shopify_customers AS shop