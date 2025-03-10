{{ config(
    partition_by={
      "field": "invoice_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "house_bill_number",
        "shipment_type", 
		"invoice_number",
        "invoice_line_item_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('wen_parker', 'invoice_line_items') }}
)

, cleaned AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key([
      'invoice_number'
      , 'house_bill_number'
      , 'charge_code'
      , 'charge_name'
      , 'invoice_date'
      , 'shipment_type'
    ]) }} AS invoice_line_item_id
        , invoice_number
        , PARSE_DATE('%Y%m%d' , invoice_date) AS invoice_date
        , payer_code
        , payer_name
        , TRIM(LOWER(shipment_type)) AS shipment_type
        , house_bill_number
        , TRIM(LOWER(charge_code)) AS charge_code
        , TRIM(LOWER(charge_name)) AS charge_name
        , invoice_currency
        , CAST(REPLACE(REPLACE(invoice_amount , ',' , '') , '$' , '') AS FLOAT64)
            AS amount
        , source_synced_at
    FROM source
)

/*
	This model MUST be deduplicated by unique records. This is because each file contains data for
	the last 90 days, and the same invoice may appear in multiple files.
*/
SELECT
    *
FROM cleaned
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY invoice_line_item_id
        ORDER BY source_synced_at DESC
    ) = 1
