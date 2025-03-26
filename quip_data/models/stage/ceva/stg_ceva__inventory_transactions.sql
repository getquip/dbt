{{ config(
    partition_by={
      "field": "adjusted_at",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    },
	cluster_by=[
        "adjustment_category",
        "sku", 
		"batch_id",
        "adjustment_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('ceva', 'inventory_transactions') }}
	WHERE TIMESTAMP_TRUNC(SAFE_CAST(complete_dstamp AS TIMESTAMP) , DAY) >= TIMESTAMP("2025-03-15") -- remove after development
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, cleaned AS (
    SELECT
        site_id AS warehouse_id
        , 'Fontana, CA' AS warehouse_name
		, REGEXP_REPLACE(sku_id, r'\D', '') AS sku -- remove non-numeric characters
        , `key` AS adjustment_id
        , LOWER(`description`) AS adjustment_name
        , SAFE_CAST(update_qty AS NUMERIC) AS quantity
        , UPPER(reason_id) AS adjustment_category
        , LOWER(reason_desc) AS adjustment_reason
        , batch_id
        , source_file_name
        , source_synced_at
        , 'ceva' AS provider
        , SAFE_CAST(to_date AS DATE) AS to_date
        , SAFE_CAST(from_date AS DATE) AS from_date
        , SAFE_CAST(complete_dstamp AS TIMESTAMP) AS adjusted_at
        , LOWER(code) AS adjustment_reason_type
    FROM source
)

SELECT
    * EXCEPT(quantity, adjustment_reason, adjustment_category, adjustment_reason_type)
    , TIMESTAMP_TRUNC(adjusted_at, HOUR) AS adjustment_hour
    , ROW_NUMBER() OVER (PARTITION BY adjustment_id ORDER BY adjusted_at DESC) AS adjustment_sequence
    /*
    quantity is negative in Newgistics when it's a deduction.
    CEVA reports update_qty as a positive number for Shipment, this flips that
    */
    , IF(adjustment_reason_type = 'shipment' , -quantity , quantity) AS quantity
    /* Note that these CASE WHEN statements are chronological */
    , CASE
        -- by adjustment_category
        WHEN adjustment_category IN ('EXPD', 'EXPIRED') OR adjustment_reason_type = 'expiry update'THEN 'expired inventory'
        WHEN adjustment_category = 'EX_BS' THEN 'excess backstock'
        WHEN adjustment_category IN ('DMGD', 'DISTDAM', 'DAMGD') THEN 'damaged goods'
        WHEN adjustment_category = 'PIAD' THEN 'physical inventory adjustment'
        WHEN adjustment_category = 'QCHOLD' THEN 'quarantine hold'
        WHEN adjustment_category = 'EX_NE' THEN 'export - no entry'
        WHEN adjustment_category = 'EX_DS' THEN 'excluded from distribution'
        WHEN adjustment_category = 'MISS' THEN 'inventory missing'
        -- by adjustment_reason_type
        WHEN adjustment_reason_type IN ('pick') THEN 'oms allocation/incoming change'
        WHEN adjustment_reason_type = 'stock check' THEN 'inventory daily sync'
        WHEN adjustment_reason_type = 'inv unlock' THEN 'inventory adjustment correction'
        WHEN adjustment_reason_type = 'cond update' THEN 'condition update'
        WHEN  adjustment_reason_type = 'replenish' THEN 'replenishment'
        WHEN adjustment_reason_type = 'receipt' THEN 'receipt confirmation'
        WHEN adjustment_reason_type = 'qc hold' THEN 'quarantine hold'
        WHEN adjustment_reason_type = 'qc release' THEN 'quarantine released'
        -- based on adjustment_reason
        WHEN adjustment_reason = 'dist-damaged' THEN 'damaged goods'
        ELSE COALESCE(adjustment_reason, adjustment_reason_type)
    END AS adjustment_reason
    , CASE
        -- sampling
        WHEN adjustment_category = 'SMPL' THEN 'sampling'
        WHEN adjustment_reason_type = 'preadv line' THEN 'sampling'
        -- locked and unlocked
        WHEN adjustment_category = 'LOCK' THEN 'locked'
        WHEN adjustment_reason_type = 'inv lock' THEN 'locked'
        WHEN adjustment_category = 'MISS' AND adjustment_reason_type = 'inv lock' THEN 'locked'
        WHEN adjustment_reason_type = 'inv unlock' THEN 'unlocked'
        -- damaged goods
        WHEN adjustment_category IN ('DMGD', 'DISTDAM', 'DAMGD', 'DSAD') THEN 'damaged goods'
        -- inventory re-organization
        WHEN adjustment_category = 'SKUCONV' THEN 'inventory re-organization'
        WHEN adjustment_reason_type IN ('tag swap', 'kit build', 'unkit', 'relocate', 'repack', 'sort putaway') THEN 'inventory re-organization'
        -- allocated and deallocated
        WHEN adjustment_reason_type IN ('allocate', 'soft allocate', 'pick', 'deallocate', 'soft deallocate', 'unpick', 'trailer shipped') THEN 'allocation'
        -- receiving
        WHEN adjustment_reason_type IN ('vehicle unload', 'vehicle load', 'receipt', 'receipt reverse', 'putaway') THEN 'receiving'
        WHEN adjustment_reason = 'receiving issue adjustment' THEN 'receiving'
        -- system updates
        WHEN adjustment_reason_type = 'config update' THEN 'system update'
        -- expired inventory
        WHEN adjustment_category IN ('EXPD', 'EXPIRED') OR adjustment_reason_type = 'expiry update'THEN 'expired inventory'
        -- quarantined
        WHEN adjustment_reason_type IN ('qc hold', 'qc release') THEN 'quarantine'
        WHEN adjustment_category = 'QCHOLD' THEN 'quarantine'
        -- inventory adjustment
        WHEN adjustment_category IN ('CCAD', 'PIAD', 'RCAD') THEN 'inventory adjustment'
        WHEN adjustment_reason_type IN ('replenish', 'batch update', 'pallet update', 'stock check') THEN 'inventory adjustment'
        -- shipment
        WHEN adjustment_reason_type LIKE '%shipment%' THEN 'shipment'
        -- other
        WHEN adjustment_category = 'OTHR' THEN 'other'
        WHEN adjustment_reason_type = 'cond update' THEN 'other'
        ELSE adjustment_reason_type
    END AS adjustment_category
    , CASE
        -- based on the adjustment_reason_type
        WHEN adjustment_reason_type IN ('inv lock', 'inv unlock', 'stock check', 'replenish', 'batch update', 'expiry update'
            , 'relocate', 'qc hold', 'pallet update', 'vehicle load', 'vehicle unload', 'qc release', 'tag swap'
            , 'unkit', 'kit build', 'preadv line', 'repack', 'cond update', 'putaway', 'sort putaway') THEN 'wms adjustment'
        WHEN adjustment_reason_type IN ('soft deallocate', 'pick', 'unpick', 'soft allocate') THEN 'oms adjustment'
        WHEN adjustment_reason_type LIKE '%shipment%' THEN 'shipment'
        WHEN adjustment_reason_type IN ('deallocate', 'allocate', 'trailer shipped') THEN 'shipment'
        WHEN adjustment_reason_type IN ('receipt', 'receipt reverse') THEN 'receipt'
        -- based on the adjustment_category
        WHEN adjustment_category IN ('DMGD', 'DISTDAM', 'DAMGD', 'CCAD', 'SKUCONV', 'PIAD', 'RCAD', 'EXPD', 'EXPIRED') THEN 'wms adjustment'
        -- based on the adjustment_reason
        WHEN adjustment_reason IN ('dist-damaged', 'disposal or scrap adjustment') THEN 'wms adjustment'
        ELSE adjustment_reason_type
    END AS adjustment_reason_type
    , adjustment_reason_type AS adjustment_reason_type_raw
    , adjustment_reason AS adjustment_reason_raw
    , adjustment_category AS adjustment_category_raw
FROM cleaned
