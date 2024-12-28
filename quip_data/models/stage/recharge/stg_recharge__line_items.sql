WITH source AS (
	SELECT * FROM {{ source('recharge', 'charges') }}
)

SELECT 
  id AS charge_id
  , items.purchase_item_id AS line_item_id
  , items.external_product_id
  , items.purchase_item_type
  , items.external_variant_id
  , items.grams AS weight_per_unit_grams
  , items.handle
  , items.images
  , items.offer_attributes
  , items.original_price
  , items.unit_price_includes_tax
  , items.properties
  , items.purchase__item_type
  , items.quantity
  , items.sku
  , items.tax_due
  , items.tax_lines
  , items.taxable AS is_taxable
  , items.title
  , items.total_price
  , items.unit_price
  , items.variant_title
  , items.taxable_amount
FROM source
, UNNEST(line_items) AS items
-- dedupe charges
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1