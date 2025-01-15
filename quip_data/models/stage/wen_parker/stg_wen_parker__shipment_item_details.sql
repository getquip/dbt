WITH source AS (
	SELECT * FROM {{ source('wen_parker', 'shipment_item_details') }}
)

/*
  We need to aggregate this data to the po_number because occassionally there are 2 line items per sku.
  According to Wen Parker, the data exports we receive are missing a "priority" field that should be
  used to dedupe.

  Example: We have two rows for the sku, but each row's quantity is correct, and when aggregated,
  equals the actual PO/shipment level quantity for the sku.
    - house_bill_number = XMNA00466444
    - po_number = PO0001822
    - sku_number = 900-00110
*/

, renamed AS (
  SELECT DISTINCT
    house_bill_number
    , po_number
    , sku_number AS sku
    , CAST(cartons AS INTEGER) AS cartons
    , CAST(quantity AS INTEGER) AS quantity
  FROM source
)

SELECT
  house_bill_number
  , po_number
  , sku
  , SUM(cartons) AS cartons
  , SUM(quantity) AS quantity
FROM renamed
GROUP BY 1, 2, 3