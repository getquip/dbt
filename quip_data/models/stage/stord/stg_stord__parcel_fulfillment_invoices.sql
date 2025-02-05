WITH source AS (
    SELECT * FROM {{ source("stord", "parcel_fulfillment_invoices") }}
)


SELECT
    merchant_name
    , merchant_number
    , invoice_number
    , LOWER(warehouse_name) AS warehouse_name
    , order_number
    , drop_ship_name
    , SAFE_CAST(received_date AS DATE) AS received_date
    , tracking_number
    , pkg_reference
    , SAFE_CAST(dim_weight AS NUMERIC) AS dim_weight
    , company
    , address_1
    , address_2
    , city
    , state
    , postal_code
    , country
    , ship_from_zip
    , zone
    , LOWER(class_of_service) AS class_of_service
    , ngs_postage_key
    , source_synced_at
    , source_file_name
    , SAFE_CAST(COALESCE(no_of_pkgs, no__of_pkgs) AS INTEGER) AS num_packages
    , SAFE_CAST(shipment_id AS INTEGER) AS shipment_id
    , SAFE_CAST(total_amt AS FLOAT64) AS total_amount
    , SAFE_CAST(invoice_date AS DATE) AS invoice_date
    , SAFE_CAST(order_date AS DATE) AS order_date
    , SAFE_CAST(weight AS NUMERIC) AS weight
    , SAFE_CAST(height AS NUMERIC) AS height
    , SAFE_CAST(width AS NUMERIC) AS width
    , SAFE_CAST(length AS NUMERIC) AS length
    , COALESCE(no_of_pieces , no__of_pieces) AS no_of_pieces
    , COALESCE(contents , contents__sku_qty_sku_qty_) AS contents_sku_quantity
    , LOWER(carrier) AS carrier
    , LOWER(transmitted_shipping_method) AS transmitted_shipping_method
    , LOWER(actual_shipping_method) AS actual_shipping_method
    , SAFE_CAST(shipped_date AS DATE) AS shipped_date
    , LOWER(fee_category) AS fee_category
    , LOWER(fee_surcharge_type_1) AS fee_surcharge_type_1
    , SAFE_CAST(fee_type_charges_1 AS FLOAT64) AS fee_type_charges_1
    , LOWER(fee_surcharge_type_2) AS fee_surcharge_type_2
    , SAFE_CAST(fee_type_charges_2 AS FLOAT64) AS fee_type_charges_2
    , LOWER(fee_surcharge_type_3) AS fee_surcharge_type_3
    , SAFE_CAST(fee_type_charges_3 AS FLOAT64) AS fee_type_charges_3
    , LOWER(fee_surcharge_type_4) AS fee_surcharge_type_4
    , SAFE_CAST(fee_type_charges_4 AS FLOAT64) AS fee_type_charges_4
    , LOWER(fee_surcharge_type_5) AS fee_surcharge_type_5
    , SAFE_CAST(fee_type_charges_5 AS FLOAT64) AS fee_type_charges_5
    , LOWER(fee_surcharge_type_6) AS fee_surcharge_type_6
    , SAFE_CAST(fee_type_charges_6 AS FLOAT64) AS fee_type_charges_6
    , LOWER(fee_surcharge_type_7) AS fee_surcharge_type_7
    , SAFE_CAST(fee_type_charges_7 AS FLOAT64) AS fee_type_charges_7
    , LOWER(fee_surcharge_type_8) AS fee_surcharge_type_8
    , SAFE_CAST(fee_type_charges_8 AS FLOAT64) AS fee_type_charges_8
    , LOWER(fee_surcharge_type_9) AS fee_surcharge_type_9
    , SAFE_CAST(fee_type_charges_9 AS FLOAT64) AS fee_type_charges_9
    , LOWER(fee_surcharge_type_10) AS fee_surcharge_type_10
    , SAFE_CAST(fee_type_charges_10 AS FLOAT64) AS fee_type_charges_10
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY ngs_postage_key ORDER BY source_synced_at DESC) = 1