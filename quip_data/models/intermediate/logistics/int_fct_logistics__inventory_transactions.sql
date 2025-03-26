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

ceva AS (
	SELECT * FROM {{ ref("stg_ceva__inventory_transactions") }}
)

, stord AS (
	SELECT * FROM {{ ref("stg_stord__inventory_adjustments") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	`provider`
	, warehouse_id
	, warehouse_name
	, sku
	, adjustment_id
	, lot_number
	, adjustment_reason_type
	, adjustment_reason
	, adjustment_category
	, adjustment_quantity
	, previous_quantity
	, adjusted_to_quantity
	, adjusted_at
	, adjustment_sequence
FROM stord

UNION ALL

SELECT
	`provider`
	, warehouse_id
	, warehouse_name
	, sku
	, adjustment_id
	, NULL AS lot_number
	, adjustment_reason_type
	, adjustment_reason
	, adjustment_category
	, NULL AS adjustment_quantity
	, NULL AS previous_quantity
	, quantity AS adjusted_to_quantity
	, adjusted_at
	, adjustment_sequence
FROM ceva