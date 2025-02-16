WITH

source AS (
  SELECT * FROM {{ source("delighted_nps", "response") }}
)

SELECT  
  id AS response_id
  , _fivetran_deleted AS is_source_deleted
  , _fivetran_synced AS source_synced_at
  , properties_order_id AS order_id
  , properties_event_id AS event_id
  , properties_contains_floss AS contains_floss
  , properties_contains_refillable_dispenser AS contains_refillable_dispenser
  , properties_contains_toothpaste AS contains_toothpaste
  , properties_contains_toothbrush AS contains_toothbrush
  , properties_contains_smart_toothbrush AS contains_smart_toothbrush
  , properties_contains_rechargeable_brush AS contains_rechargeable_brush
  , properties_contains_rechargeable_smart_brush AS contains_rechargeable_smart_brush
  , properties_utm_campaign AS utm_campaign
  
  , COALESCE(
    properties_products_1_sku
    , properties_products_2_sku
    , properties_products_3_sku
    , properties_products_4_sku
    , properties_products_5_sku
    , properties_products_6_sku
    , properties_products_7_sku
    , properties_products_8_sku
    , properties_products_9_sku
    , properties_products_10_sku
    , properties_products_11_sku
    , properties_products_12_sku
    , properties_products_13_sku
  ) AS sku

  , COALESCE(
    properties_transactions_1_quantity
    , properties_transactions_2_quantity
    , properties_transactions_3_quantity
  ) AS transaction_quantity

  , COALESCE(
    properties_products_1_quantity
    , properties_products_2_quantity
    , properties_products_3_quantity
    , properties_products_4_quantity
    , properties_products_5_quantity
    , properties_products_6_quantity
    , properties_products_7_quantity
    , properties_products_8_quantity
    , properties_products_9_quantity
    , properties_products_10_quantity
    , properties_products_11_quantity
    , properties_products_12_quantity
    , properties_products_13_quantity
    , properties_products_14_quantity
    , properties_products_15_quantity
    , properties_products_16_quantity
    , properties_products_17_quantity
    , properties_products_18_quantity
  ) AS product_quantity

  , COALESCE(
    properties_products_1_material
    , properties_products_2_material
    , properties_products_3_material
    , properties_products_4_material
    , properties_products_5_material
    , properties_products_6_material
    , properties_products_7_material
    , properties_products_8_material
    , properties_products_9_material
    , properties_products_10_material
    , properties_products_11_material
  ) AS product_material

  , COALESCE(
    properties_products_1_transaction_id
    , properties_products_2_transaction_id
    , properties_products_3_transaction_id
    , properties_products_4_transaction_id
    , properties_products_5_transaction_id
    , properties_products_6_transaction_id
    , properties_products_7_transaction_id
    , properties_products_8_transaction_id
    , properties_products_9_transaction_id
    , properties_products_10_transaction_id
    , properties_products_11_transaction_id
    , properties_products_12_transaction_id
    , properties_products_13_transaction_id
    , properties_products_14_transaction_id
    , properties_products_15_transaction_id
    , properties_products_16_transaction_id
    , properties_products_17_transaction_id
    , properties_products_18_transaction_id
  ) AS transaction_id

  , COALESCE(
    properties_transactions_1_total_price
    , properties_transactions_2_total_price
    , properties_transactions_3_total_price
  ) AS total_price

  , COALESCE(
    properties_products_1_price
    , properties_products_3_price
    , properties_products_2_price
    , properties_products_4_price
    , properties_products_9_price
    , properties_products_10_price
    , properties_products_17_price
    , properties_products_6_price
    , properties_products_7_price
    , properties_products_5_price
    , properties_products_11_price
    , properties_products_8_price
    , properties_products_16_price
    , properties_products_18_price
    , properties_products_15_price
    , properties_products_12_price
    , properties_products_13_price
    , properties_products_14_price
  ) AS product_price

  , COALESCE(
    properties_products_1_category
    , properties_products_2_category
    , properties_products_3_category
    , properties_products_4_category
    , properties_products_5_category
    , properties_products_6_category
    , properties_products_8_category
    , properties_products_9_category
    , properties_products_1_categories_1
    , properties_products_1_categories_2
    , properties_products_1_categories_3
    , properties_products_2_categories_1
    , properties_products_2_categories_2
    , properties_products_2_categories_3
    , properties_products_3_categories_1
    , properties_products_3_categories_2
    , properties_products_3_categories_3
    , properties_products_4_categories_1
    , properties_products_4_categories_2
    , properties_products_4_categories_3
    , properties_products_5_categories_1
    , properties_products_5_categories_2
    , properties_products_5_categories_3
    , properties_products_6_categories_1
    , properties_products_6_categories_2
    , properties_products_6_categories_3
    , properties_products_7_categories_1
    , properties_products_8_categories_1
    , properties_products_9_categories_1
    , properties_products_9_categories_2
    , properties_products_9_categories_3
    , properties_products_9_categories_4
    , properties_products_10_categories_1
    , properties_products_10_categories_2
    , properties_products_10_categories_3
    , properties_products_11_categories_1
    , properties_products_12_categories_1
    , properties_products_14_categories_1
    , properties_products_14_categories_2
    , properties_products_14_categories_3
  ) AS product_category

  , COALESCE(
    properties_products_1_delivery_category
    , properties_products_2_delivery_category
    , properties_products_3_delivery_category
    , properties_products_4_delivery_category
    , properties_products_5_delivery_category
    , properties_products_6_delivery_category
    , properties_products_7_delivery_category
    , properties_products_8_delivery_category
    , properties_products_9_delivery_category
    , properties_products_10_delivery_category
    , properties_products_11_delivery_category
  ) AS delivery_category
FROM source