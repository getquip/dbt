WITH source AS (
	SELECT * FROM {{ source('wen_parker', 'invoice_line_items') }}
)
/*
	This model MUST be deduplicated by unique records. This is because each file contains data for
	the last 90 days, and the same invoice may appear in multiple files.
*/
SELECT DISTINCT
  invoice_number
  , PARSE_DATE('%Y%m%d', invoice_date) AS invoice_date
  , payer_code
  , payer_name
  , TRIM(LOWER(shipment_type)) AS shipment_type
  , house_bill_number
  , charge_code
  , charge_name
  , invoice_currency
  , CAST(REPLACE(REPLACE(invoice_amount, ',',''), '$','') AS FLOAT64) AS invoice_amount
FROM source