SELECT
  "legacy" AS version
  , subscription_id
  , user_id AS shopify_customer_id
  , DATE(created_at) AS created_date
  , DATE(canceled_at) AS canceled_date
  , address_id
  , subscription_status AS status
  , quantity
FROM `quip-etl-data.dwh.recharge_subscriptions_base` AS legacy

UNION ALL

SELECT
  "WIP" AS version
  , subscription_id
  , shopify_customer_id
  , DATE(created_at) AS created_at
  , DATE(cancelled_at) AS cancelled_at
  , address_id
  , status
  , quantity
FROM {{ ref("int_dim_subscriptions") }}
