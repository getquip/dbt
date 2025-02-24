{{ config(
    partition_by={
      "field": "transaction_date",
      "data_type": "date",
      "granularity": "day"
    },
	cluster_by=[
        "fulfillment_mode",
        "order_number", 
		"invoice_number",
        "fulfillment_invoice_id"
    ]
) }}

WITH 

source AS (
    SELECT * FROM {{ source("stord", "fulfillment_invoices") }}
)


SELECT
    {{
            dbt_utils.generate_surrogate_key([
                "invoice_number",
                "order_number",
                "trxn_date",
            ])
        }} AS fulfillment_invoice_id
    , merchant_number
    , invoice_number
    , po AS po_number
    , manifest_id
    , LOWER(warehouse) AS warehouse_name
    , order_number
    , drop_ship_name
    , transaction_num
    , source_synced_at
    , source_file_name
    , SAFE_CAST(package_count AS INTEGER) AS package_count
    , LOWER(fulfillment_mode) AS fulfillment_mode
    , SAFE_CAST(shipment_id AS INTEGER) AS shipment_id
    , LOWER(COALESCE(merchant_name , client_name , brand_name)) AS merchant_name
    , SAFE_CAST(invoice_date AS DATE) AS invoice_date
    , SAFE_CAST(received_manifest_date AS DATE) AS received_manifest_date
    , SAFE_CAST(shipment_received_date AS DATE) AS shipment_received_date
    , SAFE_CAST(closed_manifest_date AS DATE) AS closed_manifest_date
    , SAFE_CAST(trxn_date AS DATE) AS transaction_date
    , SAFE_CAST(ship_on_date AS DATE) AS ship_on_date
    , LOWER(fee_surcharge_category) AS fee_surcharge_category
    , SAFE_CAST(total_amt AS NUMERIC) AS total_amount
    , LOWER(fee_surcharge_type_1) AS fee_surcharge_type_1
    , SAFE_CAST(fee_type_charges_1 AS NUMERIC) AS fee_type_charges_1
    , LOWER(fee_surcharge_type_2) AS fee_surcharge_type_2
    , SAFE_CAST(fee_type_charges_2 AS NUMERIC) AS fee_type_charges_2
    , LOWER(fee_surcharge_type_3) AS fee_surcharge_type_3
    , SAFE_CAST(fee_type_charges_3 AS NUMERIC) AS fee_type_charges_3
    , LOWER(fee_surcharge_type_4) AS fee_surcharge_type_4
    , SAFE_CAST(fee_type_charges_4 AS NUMERIC) AS fee_type_charges_4
    , LOWER(fee_surcharge_type_5) AS fee_surcharge_type_5
    , SAFE_CAST(fee_type_charges_5 AS NUMERIC) AS fee_type_charges_5
    , LOWER(fee_surcharge_type_6) AS fee_surcharge_type_6
    , SAFE_CAST(fee_type_charges_6 AS NUMERIC) AS fee_type_charges_6
    , LOWER(fee_surcharge_type_7) AS fee_surcharge_type_7
    , SAFE_CAST(fee_type_charges_7 AS NUMERIC) AS fee_type_charges_7
    , LOWER(fee_surcharge_type_8) AS fee_surcharge_type_8
    , SAFE_CAST(fee_type_charges_8 AS NUMERIC) AS fee_type_charges_8
FROM source
WHERE invoice_number IS NOT NULL
     AND order_number IS NOT NULL
     AND trxn_date IS NOT NULL
-- dedupe
QUALIFY ROW_NUMBER() OVER (PARTITION BY fulfillment_invoice_id ORDER BY source_file_name DESC) = 1