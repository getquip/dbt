SELECT
    "legacy" AS version
    , subscription_id
    , user_id AS customer_id
    , DATE(created_at) AS created_date
    , DATE(canceled_at) AS canceled_date
    , address_id
    , subscription_status AS status
    , quantity
FROM `quip-etl-data.dwh.recharge_subscriptions_base`

UNION ALL

SELECT
    "WIP" AS version
    , subscription_id
    , COALESCE(shopify_customer_id , recharge_customer_id , legacy_customer_id) AS customer_id
    , DATE(created_at) AS created_at
    , DATE(cancelled_at) AS cancelled_at
    , address_id
    , status
    , quantity
FROM {{ ref("int_dim_subscriptions") }}
