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
	house_bill_number
	, sku
	, sku_presentment
	, 'tariff' AS fee_type
	, tariff_type AS fee_detail_1
	, tariff_number AS fee_detail_2
	, total_allocated_tariff_cost AS total_allocated_amount
FROM tariffs
WHERE tariff_type = 'hts_china'

UNION ALL

SELECT
	house_bill_number
	, sku
	, sku_presentment
	, 'tariff' AS fee_type
	, tariff_type AS fee_detail_1
	, tariff_number AS fee_detail_2
	, total_allocated_tariff_cost AS total_allocated_amount
FROM tariffs
WHERE tariff_type = 'hts'

UNION ALL

SELECT
	house_bill_number
	, sku
	, sku_presentment
	, charge_category AS fee_type
	, charge_code AS fee_detail_1
	, charge_name AS fee_detail_2
	, allocated_invoice_amount AS total_allocated_amount
FROM fees

