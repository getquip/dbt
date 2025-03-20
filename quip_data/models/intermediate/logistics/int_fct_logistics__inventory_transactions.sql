WITH

ceva AS (
	SELECT * FROM {{ ref("stg_ceva__inventory_transactions") }}
)

, stord AS (
	SELECT * FROM {{ ref("stg_stord__inventory_adjustments") }}
)

SELECT
	"stord" AS provider
FROM stord

UNION ALL

SELECT
	provider
	, warehouse_id
	, warehouse_name
	, sku
	, insert_id AS adjustment_id
	, description
	, shipment_id
	, manifest_po
	, inventory_reason_id
	, inventory_reason
	, batch_id
FROM ceva