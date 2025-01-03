WITH

product_variant AS (
	SELECT * FROM {{ ref("stg_shopify__product_variant") }}
)

, component_category AS (
	SELECT * FROM {{ ref("seed__shopify_product_categorization") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	product_variant.shopify_product_id
	, product_variant.shopify_product_variant_id

	, product_variant.sku

	, product_variant.created_at
	, product_variant.updated_at
	
	-- component category
	, component_category.component_master_category
	, component_category.component_master_subcategory
	, component_category.component_category
	, component_category.component_color
	, component_category.component_material
	, component_category.component_edition
	, component_category.component_consumer
	, component_category.component_version
FROM product_variant
LEFT JOIN component_category
	ON product_variant.sku = component_category.sku

