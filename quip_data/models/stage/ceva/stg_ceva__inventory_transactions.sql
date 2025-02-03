WITH source AS (
    SELECT * FROM {{ source('ceva', 'inventory_transactions') }}
)

, cleaned AS (
    SELECT
        site_id AS warehouse_id
        , 'Fontana, CA' AS warehouse_name
        , sku_id AS sku
        , `key` AS insert_id
        , description
        , SAFE_CAST(update_qty AS INTEGER) AS quantity
        , reference_id AS shipment_id
        , purchase_order AS manifest_po
        , reason_id AS inventory_reason_id
        , reason_desc AS inventory_reason
        , batch_id
        , source_file_name
        , source_synced_at
        , 'ceva' AS provider
        , SAFE_CAST(to_date AS DATE) AS to_date
        , SAFE_CAST(from_date AS DATE) AS from_date
        , SAFE_CAST(complete_dstamp AS TIMESTAMP) AS completed_timestamp
        , LOWER(code) AS code
    FROM source
)

SELECT
    * EXCEPT(quantity)
    , CASE
        WHEN code = 'shipment' THEN 'ship'
        WHEN code = 'adjustment' THEN 'adjust'
        ELSE REGEXP_REPLACE(code , r'\s' , '_')
    END AS inventory_type
    /*
    quantity is negative in Newgistics when it's a deduction.
    CEVA reports update_qty as a positive number for Shipment, this flips that
    */
    , IF(code = 'shipment' , -quantity , quantity) AS quantity
FROM cleaned
