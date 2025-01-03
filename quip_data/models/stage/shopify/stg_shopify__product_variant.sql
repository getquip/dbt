WITH source AS (
	SELECT * FROM {{ source('shopify', 'product_variant') }}
)

SELECT
	id AS shopify_product_variant_id
	, product_id AS shopify_product_id

	, sku

	, created_at
	, updated_at
FROM
	source