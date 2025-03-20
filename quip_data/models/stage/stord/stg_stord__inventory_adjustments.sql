{{ config(
    materialized='incremental',
    unique_key='adjustment_id',
	incremental_strategy='insert_overwrite', 
    partition_by={
        "field": "adjusted_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=['adjustment_reason', 'sku', 'adjustment_sequence', 'adjustment_id']
) }}

WITH

source AS (
	SELECT * FROM {{ source("stord", "inventory_adjustments")}}
	{% if is_incremental() %}
	WHERE adjusted_at >= "{{ get_max_partition('adjusted_at') }}"
	{% endif %}
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
		, SAFE_CAST(adjustment_quantity AS INTEGER) AS adjustment_quantity
		, adjustment_sequence
		, category AS adjustment_category
		, expires_at AS expires_on
		, facility_alias
		, facility_id
		, item_id
		, ledger_sequence
		, lot_number
		, LOWER(name) AS adjustment_name
		, order_number
		, SAFE_CAST(previous_quantity AS INTEGER) AS previous_quantity
		, LOWER(reason) AS adjustment_reason
		, reason_code AS adjustment_reason_code
		, LOWER(reason_type) AS adjustment_reason_type
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, unit
		, SAFE_CAST(updated_quantity AS INTEGER) AS adjusted_to_quantity
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
		, CAST(NULL AS DATE) AS expires_at
		, warehouse AS facility_alias
		, warehouse_id AS facility_id
		, shipment_id AS item_id
		, CAST(NULL AS INTEGER) AS ledger_sequence
		, CAST(NULL AS STRING) AS lot_number
		, CAST(NULL AS STRING) AS adjustment_name
		, order_id AS order_number
		, CAST(NULL AS INTEGER) AS previous_quantity
		, inventory_reason AS adjustment_reason
		, CAST(inventory_reason_id AS INTEGER) AS adjustment_reason_code
		, inventory_type AS adjustment_reason_type
		, REGEXP_REPLACE(sku, r'\D', '') AS sku -- remove non-numeric characters
		, CAST(NULL AS STRING) AS unit
		, CAST(NULL AS INTEGER) AS adjusted_to_quantity
		, CAST(NULL AS TIMESTAMP) AS source_synced_at
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

{% if not is_incremental() %}

UNION ALL

SELECT
	newgistics_cleaned.*
	, NULL AS adjustment_reason_categorization
	, NULL AS adjustment_reason_code_text
	, NULL AS adjustment_reason_code_type
FROM newgistics_cleaned

{% endif %}