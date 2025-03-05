WITH source AS (
    SELECT * FROM {{ source('shopify', 'order_line') }}
)

SELECT
    order_id AS shopify_order_id
    , id AS shopify_line_item_id
    , CAST(product_id AS INTEGER) AS shopify_product_id
    , CAST(variant_id AS INTEGER) AS shopify_product_variant_id
    , REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
    , vendor AS supplier
    , gift_card AS is_paid_by_gift_card
    , taxable AS is_taxable
    , CAST(price AS FLOAT64) AS unit_price
    , CAST(quantity AS INTEGER) AS quantity
    , grams * 0.00220462 AS weight_lbs
    , CAST(total_discount AS FLOAT64) AS total_discount
    , SAFE_CAST(CASE
        WHEN JSON_EXTRACT_SCALAR(properties , '$[0].name') = 'legacy_subscription_id'
            THEN JSON_EXTRACT_SCALAR(properties , '$[0].value')
        WHEN JSON_EXTRACT_SCALAR(properties , '$[1].name') = 'legacy_subscription_id'
            THEN JSON_EXTRACT_SCALAR(properties , '$[1].value')
        WHEN JSON_EXTRACT_SCALAR(properties , '$[2].name') = 'legacy_subscription_id'
            THEN JSON_EXTRACT_SCALAR(properties , '$[2].value')
    END AS INTEGER) AS legacy_subscription_id
FROM source
