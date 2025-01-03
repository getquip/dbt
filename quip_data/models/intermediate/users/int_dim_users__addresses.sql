WITH

shopify_addresses AS (
	SELECT * FROM {{ ref("stg_shopify__orders") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT DISTINCT -- shipping addresses
	{{
		dbt_utils.generate_surrogate_key([
			'shopify_customer_id'
			, 'shipping_address_latitude'
			, 'shipping_address_longitude'
		])
	}} AS address_id
	, shopify_customer_id
	, shipping_address_address_1 AS address_1	
	, shipping_address_address_2 AS address_2
	, shipping_address_city AS city
	, shipping_address_company AS company
	, shipping_address_country AS country
	, shipping_address_country_code AS country_code
	, shipping_address_first_name AS first_name
	, shipping_address_last_name AS last_name
	, shipping_address_latitude AS latitude
	, shipping_address_longitude AS longitude
	, shipping_address_province AS province
	, shipping_address_province_code AS province_code	
	, shipping_address_zip AS zip
FROM shopify_addresses

UNION ALL

SELECT DISTINCT -- billing addresses
	{{
		dbt_utils.generate_surrogate_key([
			'shopify_customer_id'
			, 'billing_address_latitude'
			, 'billing_address_longitude'
		])
	}} AS address_id
	, shopify_customer_id
	, billing_address_address_1 AS address_1	
	, billing_address_address_2 AS address_2
	, billing_address_city AS city
	, billing_address_company AS company
	, billing_address_country AS country
	, billing_address_country_code AS country_code
	, billing_address_first_name AS first_name
	, billing_address_last_name AS last_name
	, billing_address_latitude AS latitude
	, billing_address_longitude AS longitude
	, billing_address_province AS province
	, billing_address_province_code AS province_code	
	, billing_address_zip AS zip
FROM shopify_addresses