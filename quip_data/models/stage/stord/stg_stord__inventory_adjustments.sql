{{ config(
    partition_by={
      "field": "adjusted_at",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    },
	cluster_by=[
        "adjustment_category",
        "sku", 
		"warehouse_id",
        "adjustment_id"
    ]
) }}

WITH

source AS (
	SELECT * FROM {{ source("stord", "inventory_adjustments")}}
	WHERE TIMESTAMP_TRUNC(adjusted_at , DAY) >= TIMESTAMP("2025-03-15") -- remove after development
)

, reason_codes AS (
	SELECT * FROM {{ source("stord", "inventory_adjustment_reason_codes")}}
)

, newgistics AS (
	SELECT * FROM {{ source("stord", "newgistics_inventory_transactions")}}
	WHERE TIMESTAMP_TRUNC(SAFE_CAST(`timestamp` AS TIMESTAMP) , DAY) >= TIMESTAMP("2025-03-15") -- remove after development
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		adjustment_id
		, adjusted_at
		, facility_alias AS warehouse_name
		, facility_id AS warehouse_id
		, item_id
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, category AS adjustment_category
		, LOWER(`name`) AS adjustment_name
		, IF(reason='', NULL, reason) AS adjustment_reason
		, reason_code AS adjustment_reason_code
		, reason_type AS adjustment_reason_type
		, SAFE_CAST(previous_quantity AS NUMERIC) AS previous_quantity
		, SAFE_CAST(adjustment_quantity AS NUMERIC) AS adjustment_quantity
		, SAFE_CAST(updated_quantity AS NUMERIC) AS adjusted_to_quantity
		, adjustment_sequence
		, ledger_sequence
		, expires_at AS expires_on
		, lot_number
		, order_number
		, unit
	FROM source
)

, newgistics_cleaned AS (
	SELECT
		id AS adjustment_id
		, `timestamp` AS adjusted_at
		, warehouse AS warehouse_name
		, warehouse_id
		, shipment_id
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, LOWER(`type`) AS adjustment_category
		, CAST(NULL AS STRING) AS adjustment_name
		, LOWER(IF(inventory_reason='', NULL, inventory_reason)) AS adjustment_reason
		, inventory_reason_id AS adjustment_reason_code
		, LOWER(inventory_type) AS adjustment_reason_type
		, CAST(NULL AS NUMERIC) AS previous_quantity
		, quantity AS adjustment_quantity
		, CAST(NULL AS NUMERIC) AS adjusted_to_quantity
		, ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp ASC) AS adjustment_sequence
		, CAST(NULL AS NUMERIC) AS ledger_sequence
		, CAST(NULL AS DATE) AS expires_on
		, CAST(NULL AS STRING) AS lot_number
		, order_id AS order_number
		, CAST(NULL AS STRING) AS unit
	FROM newgistics
)

, unified AS (
	SELECT
		cleaned.* EXCEPT(adjustment_category, adjustment_reason, adjustment_reason_type, adjustment_reason_code)
		, "stord" AS provider
		, LOWER(COALESCE(cleaned.adjustment_category, reason_codes.categorization)) AS adjustment_category
		, LOWER(COALESCE(cleaned.adjustment_reason_type, reason_codes.type)) AS adjustment_reason_type
		, LOWER(COALESCE(cleaned.adjustment_reason, reason_codes.text)) AS adjustment_reason
		, CAST(cleaned.adjustment_reason_code AS STRING) AS adjustment_reason_code
	FROM cleaned
	LEFT JOIN reason_codes
		ON cleaned.adjustment_reason_code = reason_codes.code

	UNION ALL

	SELECT
		newgistics_cleaned.* EXCEPT(adjustment_category, adjustment_reason, adjustment_reason_type, adjustment_reason_code)
		, "newgistics" AS provider
		, CASE 
			WHEN adjustment_category = 'receive' THEN 'receiving'
			WHEN adjustment_category IN ('adjust', 'return', 'assembly') THEN 'inventory adjustment'
			WHEN adjustment_category IN ('transfer', 'warehousetransfer', 'producttransfer') THEN 'inventory re-organization'
			WHEN adjustment_category = 'ship' THEN 'allocation'
			ELSE adjustment_category
		END AS adjustment_category
		, adjustment_reason_type
		, CASE 
			WHEN adjustment_category = 'ship' AND adjustment_quantity > 0 THEN 'inbound'
			WHEN adjustment_category = 'ship' AND adjustment_quantity < 0 THEN 'outbound shipment'
			ELSE adjustment_reason
		END AS adjustment_reason
		, adjustment_reason_code
	FROM newgistics_cleaned
)

SELECT
	* EXCEPT(adjustment_reason, adjustment_reason_type, adjustment_category)
    , TIMESTAMP_TRUNC(adjusted_at, HOUR) AS adjustment_hour
	, CASE
		WHEN adjustment_reason LIKE 'cycle count%' THEN 'cycle count adjustment'
		WHEN adjustment_reason LIKE 'physical count on%' THEN 'physical inventory adjustment'
		WHEN adjustment_reason LIKE 'other%' THEN 'other'
		WHEN adjustment_reason_type = 'expired qty' THEN 'expired inventory'
		ELSE COALESCE(adjustment_reason, adjustment_reason_type)
	END AS adjustment_reason
	, CASE
		-- wms adjustment
		WHEN adjustment_reason_type LIKE 'cycle count%' THEN 'wms adjustment'
		WHEN adjustment_reason LIKE 'physical count on%' THEN 'wms adjustment'
		WHEN adjustment_reason_type IN ('expired qty', 'sku conversion', 'quarantine qty', 'current qty') THEN 'wms adjustment'
		-- oms adjustment
		WHEN adjustment_reason_type IN ('order cancel', 'receiving error') THEN 'oms adjustment'
		WHEN adjustment_reason_type LIKE 'other%' THEN 'other'
		ELSE adjustment_reason_type
	END AS adjustment_reason_type
	, CASE
		WHEN adjustment_reason_type LIKE 'other%' AND adjustment_category IS NULL THEN 'other'
		-- expired inventory
		WHEN adjustment_reason_type = 'expired qty' THEN 'expired inventory'
		-- quarantine
		WHEN adjustment_category = 'quarantined' THEN 'quarantine'
		WHEN adjustment_reason_type = 'quarantine qty' THEN 'quarantine'
		-- inventory adjustment
		WHEN adjustment_reason LIKE 'physical count on%' THEN 'inventory adjustment'
		WHEN adjustment_reason_type LIKE 'cycle count%' THEN 'inventory adjustment'
		WHEN adjustment_category = 'other' 
			AND adjustment_reason IN ('work order inventory adjustment', 'inventory daily sync' , 'inventory cycle count', 'customer returned inventory') 
			THEN 'inventory adjustment'
        -- allocation
		WHEN adjustment_category = 'allocated' THEN 'allocation'
		-- receiving
		WHEN adjustment_reason_type = 'receiving error' THEN 'receiving'
		-- other
		WHEN adjustment_reason_type LIKE '%correction%' AND adjustment_category IS NULL THEN 'other'
		-- damaged goods
		WHEN adjustment_reason_type = 'damaged' THEN 'damaged goods'
		ELSE adjustment_category
	END AS adjustment_category
	, adjustment_reason_type AS adjustment_reason_type_raw
	, adjustment_reason AS adjustment_reason_raw
	, adjustment_category AS adjustment_category_raw
FROM unified
QUALIFY ROW_NUMBER() OVER (PARTITION BY adjustment_id ORDER BY adjusted_at DESC) = 1
