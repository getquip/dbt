WITH source AS (
    SELECT * FROM {{ source('recharge', 'orders') }}
)

, subscriptions AS (
    SELECT * FROM {{ ref('stg_recharge__subscriptions') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
    SELECT
        id AS recharge_order_id
        , CAST(external_order_id.ecommerce AS INTEGER) AS shopify_order_id
        , customer.id AS recharge_customer_id
        , address_id AS recharge_address_id
        , is_prepaid
        , processed_at
        , scheduled_at
        , created_at
        , status
        , subtotal_price
        , tags
        , source.taxable
        , source.total_discounts
        , source.total_price
        , source.total_tax
        , updated_at
        , total_line_items_price
        , total_weight_grams
        , total_refunds
        , type -- 'checkout' are the first orders of a subscription
        , total_duties
        , line_items
    FROM source
)
SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY recharge_order_id ORDER BY updated_at DESC) = 1