WITH source AS (
    SELECT * FROM {{ ref("stg_recharge__orders") }}
)

, cleaned AS (
    SELECT
        source.recharge_order_id
        , source.shopify_order_id
        , source.recharge_address_id
        , source.recharge_customer_id
        , items.sku
        , items.properties
        , items.handle
        , items.images
        , items.offer_attributes
        , items.tax_lines
        , items.taxable AS is_taxable
        , items.unit_price_includes_tax AS is_unit_price_includes_tax
        , items.grams AS weight_per_unit_grams
        , items.purchase_item_id
        , items.purchase_item_type
        , CAST(items.quantity AS INTEGER) AS quantity
        , CAST(items.total_price AS FLOAT64) AS total_price
        , CAST(items.unit_price AS FLOAT64) AS unit_price
        , CAST(items.taxable_amount AS FLOAT64) AS taxable_amount
        , CAST(items.original_price AS FLOAT64) AS original_price -- price without discount or taxes
        , CAST(items.tax_due AS FLOAT64) AS total_tax
        , LOWER(items.title) AS title
        , LOWER(items.variant_title) AS variant_title
        , CAST(items.external_product_id.ecommerce AS INTEGER) AS shopify_product_id
        , CAST(items.external_variant_id.ecommerce AS INTEGER) AS shopify_product_variant_id
    FROM source
    , UNNEST(line_items) AS items
    
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'shopify_order_id'
        , 'shopify_product_id'
        , 'shopify_product_variant_id'
        , 'sku'
        , 'purchase_item_id'
    ]) }} AS line_item_id
    , total_price - original_price - total_tax AS total_discount 
    , IF(purchase_item_type = 'subscription', purchase_item_id, NULL) AS subscription_id
    , *
FROM cleaned