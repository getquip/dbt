WITH
orders AS (
    SELECT * FROM {{ stg_shopify__orders}}
)

, legacy_orders AS (
    SELECT 'quip_public.orders'
)

SELECT
    order_id
    , user_id

    , created_at
    , updated_at
    , cancelled_at

    , 'shopify' AS data_source
    , fulfillment_status
    , payment_status



FROM orders

-- order_type
-- fulfillment_status
-- payment_status
-- is_only_accessories_or_refills