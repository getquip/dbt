
WITH

shipment_items AS (
	-- at the sku level
	SELECT * FROM {{ ref("stg_wen_parker__shipment_item_details") }}
)

, invoice_items AS (
	-- at the invoice and charge code level
	SELECT * FROM {{ ref("stg_wen_parker__invoice_line_items") }}
)

, charge_codes AS (
	SELECT * FROM {{ ref("seed__wen_parker_charge_codes") }}
)

, duties AS (
	SELECT * FROM {{ ref("stg_wen_parker__hts_duties") }}
)

, tariffs AS (
	SELECT * FROM {{ ref("stg_wen_parker__tariffs") }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, house_bill_item_summary AS (
  SELECT
		items.house_bill_number
		, items.sku
		, duties.tariff_number
		-- quantity
		, items.quantity AS total_sku_quantity
		, SUM(items.quantity) OVER (PARTITION BY items.house_bill_number) AS total_hbl_item_quantity 
		-- weight
		, duties.weight_lb * items.total_sku_quantity AS total_sku_weight_lb
		, SUM(duties.weight_lb * items.total_sku_quantity) OVER (PARTITION BY items.house_bill_number) AS total_hbl_weight_lb
		-- duties
		, duties.value_per_unit AS value_per_unit -- need to coalesce with cogs with this as secondary
		, (total_sku_quanty * value_per_unit * duty_rate) +  (total_sku_quanty * value_per_unit * china_duty_rate) AS total_sku_duties
	FROM shipment_items AS items
	LEFT JOIN duties
  		ON items.sku = duties.sku

)

, allocations AS (
	SELECT
		house_bill_number
		, sku
		, total_sku_quantity
		, total_hbl_item_quantity
		, total_sku_weight_lb
		, total_hbl_weight_lb
		, SAFE_DIVIDE(total_sku_quantity, total_hbl_item_quantity) AS allocation_percentage_by_quantity
		, SAFE_DIVIDE(total_sku_weight_lb, total_hbl_weight_lb) AS allocation_percentage_by_weight
		, SAFE_DIVIDE(total_sku_duties, SUM(total_sku_duties) OVER (PARTITION BY house_bill_number)) AS allocation_percentage_by_duties
	FROM house_bill_item_summary
)

SELECT
  items.*
  , charge_codes.* EXCEPT(charge_type)
  , invoice_items.invoice_number
  , invoice_items.amount
  , CASE
  		WHEN charge_codes.charge_type = 'freight'
		THEN items.allocation_percentage_by_weight * invoice_items.amount 
		ELSE items.allocation_percentage_by_quantity * invoice_items.amount 
	END AS allocated_amount
, tariffs.total_tariff_duty * items.allocation_percentage_by_duties AS allocated_duties
, tariffs.total_tariff_cost * items.allocation_percentage_by_duties AS allocated_tariff_cost
FROM allocations AS items
LEFT JOIN  invoice_items
  ON items.house_bill_number = invoice_items.house_bill_number
LEFT JOIN charge_codes
  ON invoice_items.charge_code = charge_codes.charge_code
  AND invoice_items.charge_name = charge_codes.charge_name
LEFT JOIN tariffs
  ON items.house_bill_number = tariffs.house_bill_number
  AND items.tariff_number = tariffs.tariff_number
