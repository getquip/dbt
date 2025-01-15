WITH source AS (
	SELECT * FROM {{ source('wen_parker', 'shipment_item_details') }}
)

SELECT DISTINCT
  house_bill_number
  , po_number
  , sku_number AS sku
  , CAST(cartons AS INTEGER) AS cartons
  , CAST(quantity AS INTEGER) AS quantity
FROM source