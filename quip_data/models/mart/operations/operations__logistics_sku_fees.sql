WITH

fees AS (
	SELECT * FROM {{ ref("int_fct_logistics__house_bill_sku_fees") }}
)

, tariffs AS (
	SELECT * FROM {{ ref("int_fct_logistics__house_bill_sku_tariffs") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------


SELECT
	{{ dbt_utils.generate_surrogate_key(['house_bill_number', 'sku', 'tariff_type', 'tariff_number']) }} as id
	, house_bill_number
	, sku
  	, REPLACE(sku, 'R', '') AS sku_presentment
	, 'tariff' AS fee_type
	, tariff_type AS fee_detail_1
	, tariff_number AS fee_detail_2
	, SUM(total_allocated_tariff_cost) AS total_allocated_amount
FROM tariffs
WHERE tariff_type = 'hts_china'
GROUP BY 1,2,3,4,5,6,7

UNION ALL

SELECT
	{{ dbt_utils.generate_surrogate_key(['house_bill_number', 'sku', 'tariff_type', 'tariff_number']) }} as id
	, house_bill_number
	, sku
  	, REPLACE(sku, 'R', '') AS sku_presentment
	, 'tariff' AS fee_type
	, tariff_type AS fee_detail_1
	, tariff_number AS fee_detail_2
	, SUM(total_allocated_tariff_cost) AS total_allocated_amount
FROM tariffs
WHERE tariff_type = 'hts'
GROUP BY 1,2,3,4,5,6,7

UNION ALL

SELECT
	{{ dbt_utils.generate_surrogate_key(['house_bill_number', 'sku', 'charge_category', 'charge_code', 'charge_name']) }} as id	
	, house_bill_number
	, sku
  	, REPLACE(sku, 'R', '') AS sku_presentment
	, charge_category AS fee_type
	, charge_code AS fee_detail_1
	, charge_name AS fee_detail_2
	, SUM(allocated_invoice_amount) AS total_allocated_amount
FROM fees
GROUP BY 1,2,3,4,5,6,7