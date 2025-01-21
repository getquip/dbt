WITH source AS (
	SELECT * FROM {{ source('shopify', 'order_line') }}
)

SELECT
	order_id AS shopify_order_id
	, id AS shopify_line_item_id
	, product_id AS shopify_product_id
	, variant_id AS shopify_variant_id
	, price
	, quantity
	, sku
FROM source