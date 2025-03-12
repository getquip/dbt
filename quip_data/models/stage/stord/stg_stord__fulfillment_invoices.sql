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

, cleaned AS (
    SELECT
        merchant_number
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
        -- parse dates
        , COALESCE(
            SAFE_CAST(invoice_date AS DATE) -- Format: 2024-09-09
            , PARSE_DATE('%m/%d/%y', invoice_date) -- Format: 9/9/24
            , SAFE.PARSE_DATE('%m/%d/%Y', invoice_date)  -- Format: 9/9/2024
        ) AS invoice_date
        , COALESCE(
            SAFE_CAST(closed_manifest_date AS DATE)
            , PARSE_DATE('%m/%d/%y', closed_manifest_date)
            , SAFE.PARSE_DATE('%m/%d/%Y', closed_manifest_date)
        ) AS closed_manifest_date
        , COALESCE(
            SAFE_CAST(received_manifest_date AS DATE)
            , PARSE_DATE('%m/%d/%y', received_manifest_date)
            , SAFE.PARSE_DATE('%m/%d/%Y', received_manifest_date)
        ) AS received_manifest_date
        , COALESCE(
            SAFE_CAST(shipment_received_date AS DATE)
            , PARSE_DATE('%m/%d/%y', shipment_received_date)
            , SAFE.PARSE_DATE('%m/%d/%Y', shipment_received_date)
        ) AS shipment_received_date
        , COALESCE(
            SAFE_CAST(trxn_date AS DATE)
            , PARSE_DATE('%m/%d/%y', trxn_date)
            , SAFE.PARSE_DATE('%m/%d/%Y', trxn_date)
        ) AS transaction_date
        , COALESCE(
            SAFE_CAST(ship_on_date AS DATE)
            , PARSE_DATE('%m/%d/%y', ship_on_date)
            , SAFE.PARSE_DATE('%m/%d/%Y', ship_on_date)
        ) AS ship_on_date
    FROM source
)


SELECT
    {{
        dbt_utils.generate_surrogate_key([
            "invoice_number",
            "order_number",
            "transaction_date",
        ])
    }} AS fulfillment_invoice_id
    , *
FROM cleaned
WHERE invoice_number IS NOT NULL
     AND transaction_date IS NOT NULL
-- dedupe
QUALIFY ROW_NUMBER() OVER (PARTITION BY fulfillment_invoice_id ORDER BY source_file_name DESC) = 1
