WITH 

tariffs AS (
	-- at the hbl and sku level NEED TO CONFIRM
	SELECT * FROM {{ ref("stg_wen_parker__tariff_details") }}
)

, house_bill_item_summary AS (
  SELECT * FROM {{ ref("int_fct_logistics__house_bill_item_summary")}}

)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	tariffs.house_bill_number
	, tariffs.tariff_number
	, items.sku
	, IF(items.china_tariff_number IS NULL, 'hts', 'hts_china') AS tariff_type
	, items.total_sku_quantity
	, items.total_tariff_sku_quantity
	, items.total_china_tariff_sku_quantity
	, items.allocation_percentage_for_tariffs
	, items.allocation_percentage_for_china_tariffs
	, tariffs.total_tariff_duty
	, tariffs.total_tariff_fees
	, tariffs.total_tariff_cost
	, IF(items.china_tariff_number IS NULL, 
		tariffs.total_tariff_duty * items.allocation_percentage_for_tariffs, 
		tariffs.total_tariff_duty * items.allocation_percentage_for_china_tariffs) AS total_allocated_tariff_duty 
	, IF(items.china_tariff_number IS NULL, 
		tariffs.total_tariff_fees * items.allocation_percentage_for_tariffs, 
		tariffs.total_tariff_fees * items.allocation_percentage_for_china_tariffs) AS total_allocated_tariff_fees
	, IF(items.china_tariff_number IS NULL, 
		tariffs.total_tariff_cost * items.allocation_percentage_for_tariffs, 
		tariffs.total_tariff_cost * items.allocation_percentage_for_china_tariffs) AS total_allocated_tariff_cost
FROM tariffs
LEFT JOIN house_bill_item_summary AS items
	ON tariffs.house_bill_number = items.house_bill_number
	AND (tariffs.tariff_number = items.tariff_number OR tariffs.tariff_number = items.china_tariff_number)
