WITH 

tariffs AS (
	-- at the hbl and sku level NEED TO CONFIRM
	SELECT * FROM {{ ref("stg_wen_parker__tariff_details") }}
)

, house_bill_item_summary AS (
  SELECT * FROM {{ ref("int_fct_logistics__house_bill_sku_summary")}}

)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	tariffs.house_bill_number
	, hts.po_number
	, tariffs.tariff_number
	, tariffs.tariff_bill_id
	, hts.sku
	, hts.sku_presentment
	, 'hts' AS tariff_type
	, COALESCE(hts.total_sku_quantity, 0) AS total_sku_quantity
	, COALESCE(hts.total_tariff_cost_basis, 0) AS total_tariff_cost_basis
	, COALESCE(hts.allocation_percentage_for_tariffs, 1) AS allocation_percentage_for_tariffs
	, tariffs.total_tariff_duty * COALESCE(hts.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_duty 
	, tariffs.total_tariff_fees * COALESCE(hts.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_fees 
	, tariffs.total_tariff_cost * COALESCE(hts.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_cost 
FROM tariffs
INNER JOIN house_bill_item_summary AS hts
	ON tariffs.house_bill_number = hts.house_bill_number
	AND tariffs.tariff_number = hts.tariff_number 

UNION ALL

SELECT
	tariffs.house_bill_number
	, hts_china.po_number
	, tariffs.tariff_number
	, tariffs.tariff_bill_id
	, hts_china.sku
	, hts_china.sku_presentment
	, 'hts_china' AS tariff_type
	, COALESCE(hts_china.total_sku_quantity, 0) AS total_sku_quantity
	, COALESCE(hts_china.total_tariff_cost_basis, 0) AS total_tariff_cost_basis
	, COALESCE(hts_china.allocation_percentage_for_tariffs, 1) AS allocation_percentage_for_tariffs
	, tariffs.total_tariff_duty * COALESCE(hts_china.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_duty 
	, tariffs.total_tariff_fees * COALESCE(hts_china.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_fees 
	, tariffs.total_tariff_cost * COALESCE(hts_china.allocation_percentage_for_tariffs, 1) AS total_allocated_tariff_cost 
FROM tariffs
INNER JOIN house_bill_item_summary AS hts_china
	ON tariffs.house_bill_number = hts_china.house_bill_number
	AND tariffs.tariff_number = hts_china.china_tariff_number