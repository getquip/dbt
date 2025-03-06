
WITH

invoice_items AS (
	-- at the invoice and charge code level
	SELECT * FROM {{ ref("stg_wen_parker__invoice_line_items") }}
)

, charge_codes AS (
	SELECT * FROM {{ ref("seed__wen_parker_charge_codes") }}
)

, house_bill_item_summary AS (
  SELECT * FROM {{ ref("int_fct_logistics__house_bill_item_summary")}}

)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	items.* EXCEPT(tariff_number, china_tariff_number, total_tariff_sku_quantity, total_china_tariff_sku_quantity)
	, charge_codes.*
	, invoice_items.invoice_number
	, invoice_items.amount AS total_invoice_amount
	, COALESCE(CASE
		WHEN charge_codes.charge_category = 'freight'
		THEN items.allocation_percentage_by_weight * invoice_items.amount 
		ELSE items.allocation_percentage_by_quantity * invoice_items.amount 
	END, invoice_items.amount) AS allocated_invoice_amount
FROM house_bill_item_summary AS items
-- this join filters out any house_bill_numbers that do do not have invoice items
INNER JOIN  invoice_items
  ON items.house_bill_number = invoice_items.house_bill_number
LEFT JOIN charge_codes
  ON invoice_items.charge_code = charge_codes.charge_code
  AND invoice_items.charge_name = charge_codes.charge_name