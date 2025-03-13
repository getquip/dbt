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
	, COALESCE(hts.po_number, hts_china.po_number) AS po_number
	, tariffs.tariff_number
	, COALESCE(hts_china.sku, hts.sku) AS sku
	, COALESCE(hts_china.sku_presentment, hts.sku_presentment) AS sku_presentment
	, IF(hts_china.china_tariff_number IS NULL, 'hts', 'hts_china') AS tariff_type
	, COALESCE(hts_china.total_sku_quantity, hts.total_sku_quantity, 0) AS total_sku_quantity
	, COALESCE(hts_china.total_tariff_cost_basis, hts.total_tariff_cost_basis, 0) AS total_tariff_cost_basis
	, COALESCE(hts.allocation_percentage_for_tariffs, hts_china.allocation_percentage_for_china_tariffs, 1) AS allocation_percentage_for_tariffs
	, tariffs.total_tariff_duty
	, tariffs.total_tariff_fees
	, tariffs.total_tariff_cost
	, tariffs.total_tariff_duty * COALESCE(hts.allocation_percentage_for_tariffs, hts_china.allocation_percentage_for_china_tariffs, 1) AS total_allocated_tariff_duty 
	, tariffs.total_tariff_fees * COALESCE(hts.allocation_percentage_for_tariffs, hts_china.allocation_percentage_for_china_tariffs, 1) AS total_allocated_tariff_fees 
	, tariffs.total_tariff_cost * COALESCE(hts.allocation_percentage_for_tariffs, hts_china.allocation_percentage_for_china_tariffs, 1) AS total_allocated_tariff_cost 
FROM tariffs
LEFT JOIN house_bill_item_summary AS hts
	ON tariffs.house_bill_number = hts.house_bill_number
	AND tariffs.tariff_number = hts.tariff_number 
LEFT JOIN house_bill_item_summary AS hts_china
	ON tariffs.house_bill_number = hts_china.house_bill_number
	AND tariffs.tariff_number = hts_china.china_tariff_number
