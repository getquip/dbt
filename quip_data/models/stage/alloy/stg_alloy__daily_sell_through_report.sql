{{ config(
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
	cluster_by=[
        "partner", 
		"location_number",
        "product_category",
		"product_family",
    ]
) }}

WITH

source AS (
	SELECT * FROM {{ source("alloy", "daily_sell_through_report") }}
)

, cleaned AS (
	SELECT
		partner
		, location_number
		, LOWER(product_category) AS product_category
		, LOWER(product_family) AS product_family
		, LOWER(product_color) AS product_color
		, LOWER(packaging_generation) AS packaging_generation
		, part_number
		, DATE(day) AS date
		, SAFE_CAST(unit_sales AS NUMERIC) AS unit_sales
		, SAFE_CAST(sales_retail_price AS NUMERIC) AS sales_in_dollars
		, SAFE_CAST(units_on_hand AS INTEGER) AS units_on_hand
		, SAFE_CAST(unit_sales_circular AS NUMERIC) AS unit_sales_circular
		, SAFE_CAST(sales_circular AS NUMERIC) AS sales_circular
		, SAFE_CAST(unit_sales_promo AS NUMERIC) AS unit_sales_promo
		, SAFE_CAST(sales_promo AS NUMERIC) AS sales_promo
		, SAFE_CAST(unit_sales_clearance AS NUMERIC) AS unit_sales_clearance
		, SAFE_CAST(sales_clearance AS NUMERIC) AS sales_clearance
		, SAFE_CAST(average_retail_price AS NUMERIC) AS average_retail_price
		, source_synced_at
	FROM source
)

SELECT 
	*
	, {{ dbt_utils.generate_surrogate_key([
		'partner'
		, 'location_number'
		, 'product_category'
		, 'product_family'
		, 'product_color'
		, 'packaging_generation'
		, 'part_number'
		, 'date'
	]) }} AS daily_sell_through_report_id
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_sell_through_report_id ORDER BY source_synced_at DESC) = 1