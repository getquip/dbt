/*
These subscriptions excliude subsciptions to AAA batteries.
*/
WITH

subscriptions AS (
    SELECT * FROM {{ ref("stg_recharge__subscriptions") }}
)

, legacy_subscriptions AS (
    SELECT * FROM {{ ref("stg_quip_public__subscriptions") }}
)

, customers AS (
    SELECT * FROM {{ ref("stg_recharge__customers") }}
)

, orders AS (
    SELECT * FROM {{ ref("stg_shopify__orders") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
-- need first order id
-- need period start and end dates. this is based on the latest orders charge created and next charge scheduled date

SELECT
    -- ids
    subscriptions.subscription_id
    , subscriptions.recharge_customer_id
    , customers.shopify_customer_id
    , legacy_subscriptions.legacy_customer_id
    , subscriptions.address_id
    -- cancellations
    , COALESCE(legacy_subscriptions.created_at , subscriptions.created_at) AS created_at
    , subscriptions.cancelled_at

    , subscriptions.status
    , subscriptions.quantity

FROM subscriptions
LEFT JOIN legacy_subscriptions
    ON subscriptions.legacy_subscription_id = legacy_subscriptions.legacy_subscription_id
LEFT JOIN customers
    ON subscriptions.recharge_customer_id = customers.recharge_customer_id

UNION ALL

SELECT
    -- ids
    legacy_subscriptions.legacy_subscription_id AS subscription_id
    , NULL AS recharge_customer_id
    , NULL AS shopify_customer_id
    , legacy_customer_id
    , address_id
    -- cancellations
    , created_at
    , cancelled_at
    , CAST(status AS STRING) AS status
    , quantity
FROM legacy_subscriptions
WHERE legacy_subscription_id NOT IN (SELECT legacy_subscription_id FROM subscriptions)

-- attributed order channel
-- attributed retail partner
