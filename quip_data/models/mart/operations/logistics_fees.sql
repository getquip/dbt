WITH

costs AS (
	SELECT * FROM {{ ref("int_fct_logistics__shipment_item_fee_allocation")}}
)

SELECT
	house_bill_number
	, sku
	, 'tariff' AS fee_type
	, 'hts_china' AS fee_detail_1
	, china_tariff_number AS fee_detail_2
	, 'fixed' AS fee_detail_3
	, SUM(allocated_china_tariff_cost + allocated_china_duties) AS total_allocated_amount
FROM costs
WHERE allocated_china_tariff_cost + allocated_china_duties > 0
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
	house_bill_number
	, sku
	, 'tariff' AS fee_type
	, 'hts' AS fee_detail_1
	, tariff_number AS fee_detail_2
	, 'fixed' AS fee_detail_3
	, SUM(allocated_tariff_cost + allocated_duties) AS total_allocated_amount
FROM costs
WHERE allocated_tariff_cost + allocated_duties > 0
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
	house_bill_number
	, sku
	, charge_category AS fee_type
	, charge_code AS fee_detail_1
	, tariff_number AS fee_detail_2
	, charge_type AS fee_detail_3
	, SUM(allocated_invoice_amount) AS total_allocated_amount
FROM costs
WHERE allocated_invoice_amount > 0
GROUP BY 1,2,3,4,5,6

