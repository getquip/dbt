WITH

source AS (
	SELECT * FROM {{ source("stord", "inventory_adjustments")}}
)

, reason_codes AS (
	SELECT * FROM {{ source("stord", "inventory_adjustment_reason_codes")}}
)

, newgistics AS (
	SELECT * FROM {{ source("stord", "newgistics_inventory_transactions")}}
)

, cleaned AS (
	SELECT
		adjustment_id
		, adjusted_at
		, adjustment_quantity
		, adjustment_sequence
		, category AS adjustment_category
		, expires_at
		, facility_alias
		, facility_id
		, item_id
		, ledger_sequence
		, lot_number
		, LOWER(name) AS adjustment_name
		, order_number
		, previous_quantity
		, LOWER(reason) AS adjustment_reason
		, reason_code AS adjustment_reason_code
		, LOWER(reason_type) AS adjustment_reason_type
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, unit
		, updated_quantity AS adjusted_to_quantity
		, source_synced_at
	FROM source
)

, newgistics_cleaned AS (
	SELECT
		id AS adjustment_id
		, timestamp AS adjusted_at
		, quantity AS adjustment_quantity
		, ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp ASC) AS adjustment_sequence
		, `type` AS adjustment_category
		, NULL AS expires_at
		, warehouse AS facility_alias
		, warehouse_id AS facility_id
		, shipment_id AS item_id
		, NULL AS ledger_sequence
		, NULL AS lot_number
		, NULL AS adjustment_name
		, order_id AS order_number
		, NULL AS previous_quantity
		, inventory_reason AS adjustment_reason
		, inventory_reason_id AS adjustment_reason_code
		, inventory_type AS adjustment_reason_type
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, NULL AS unit
		, NULL AS adjusted_to_quantity
		, NULL AS source_synced_at
	FROM newgistics
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adjustment_id ORDER BY adjusted_at DESC) = 1
)

SELECT
	cleaned.*
	, reason_codes.categorization AS adjustment_reason_categorization
	, reason_codes.text AS adjustment_reason_code_text
	, reason_codes.type AS adjustment_reason_code_type
FROM cleaned
LEFT JOIN reason_codes
	ON cleaned.adjustment_reason_code = reason_codes.code

UNION ALL

SELECT
	newgistics_cleaned.*
	, NULL AS adjustment_reason_categorization
	, NULL AS adjustment_reason_code_text
	, NULL AS adjustment_reason_code_type
FROM newgistics_cleaned