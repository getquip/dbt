WITH source AS (
    SELECT * FROM {{ source('recharge', 'orders') }}
)

SELECT
    id AS recharge_order_id
    , external_order_id.ecommerce AS shopify_order_id
    , is_prepaid
    , processed_at
    , scheduled_at
    , created_at
    , status
    , subtotal_price
    , tags
    , taxable
    , total_discounts
    , total_price
    , total_tax
    , updated_at
    , total_line_items_price
    , total_weight_grams
    , total_refunds
    , type
    , total_duties

    -- parse order_attributes  
    , up_client.value AS up_client_id
    , utm_client.value AS utm_client_id
    , journey.value AS journey_id
    , segment.value AS segment_client_id
FROM source
LEFT JOIN UNNEST(`order_attributes`) AS up_client
    ON up_client.name = '_up-clientID'
LEFT JOIN UNNEST(`order_attributes`) AS utm_client
    ON utm_client.name = 'utm_client'
LEFT JOIN UNNEST(`order_attributes`) AS journey
    ON journey.name = 'journey_id'
LEFT JOIN UNNEST(`order_attributes`) AS segment
    ON segment.name = '_segment-clientID'
