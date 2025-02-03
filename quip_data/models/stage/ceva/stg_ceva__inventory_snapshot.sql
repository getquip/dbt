WITH source AS (
	SELECT * FROM {{ source('ceva', 'inventory_snapshot') }}
)

, renamed AS (
	SELECT 
		CAST(snapshot_date_time AS TIMESTAMP) AS snapshot_timestamp
		, snapshot_date
		, sku_id AS sku
		, description AS sku_description
		, COALESCE(CAST(putaway_qty AS INTEGER), 0) AS putaway_quantity
		, COALESCE(CAST(quarantine AS INTEGER), 0) AS quarantine_quantity
		, COALESCE(CAST(open_order_qty AS INTEGER), 0) AS pending_quantity
		, COALESCE(CAST(expired AS INTEGER), 0) AS expired_quantity
		, COALESCE(CAST(damage AS INTEGER), 0) AS damaged_quantity
		, COALESCE(CAST(total_on_hand AS INTEGER), 0) AS total_on_hand_quantity
		, COALESCE(CAST(order_delta AS INTEGER), 0) AS sellable_quantity
		, COALESCE(CAST(kitting_qty AS INTEGER), 0) AS kitting_quantity
		, source_file_name
		, source_synced_at 
		, 'ceva' AS provider
		, 'US-FON-01' AS warehouse_name
	FROM source
)

SELECT
	*
	, total_on_hand_quantity 
		- damaged_quantity 
		- expired_quantity 
		- quarantine_quantity 
		- putaway_quantity 
		AS current_quantity
	, total_on_hand_quantity 
		- damaged_quantity 
		- expired_quantity 
		- quarantine_quantity 
		- putaway_quantity 
		- kitting_quantity 
		- pending_quantity
		AS available_quantity
FROM renamed