
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

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, house_bill_item_summary AS (
  SELECT
		house_bill_number
		, sku
		, quantity AS total_sku_quantity
		, SUM(quantity) OVER (PARTITION BY house_bill_number) AS total_hbl_item_quantity -- total quantity of all items in the house bill
		, SAFE_DIVIDE(quantity, SUM(quantity) OVER (PARTITION BY house_bill_number)) AS allocation_percentage
	FROM shipment_items

)

SELECT
  items.*
  , charge_codes.* EXCEPT(charge_type)
  , invoice_items.amount
  , items.allocation_percentage * invoice_items.amount AS allocated_amount
FROM house_bill_item_summary AS items
INNER JOIN  invoice_items
  ON items.house_bill_number = invoice_items.house_bill_number
INNER JOIN charge_codes
  ON invoice_items.charge_code = charge_codes.charge_code
  AND invoice_items.charge_name = charge_codes.charge_name