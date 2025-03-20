{{ config(
    materialized='incremental',
    unique_key='pre_advice_uuid',
	incremental_strategy='insert_overwrite', 
    partition_by={
        "field": "actual_timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=['sku', 'tracking_number', 'po_number', 'pre_advice_id']
) }}

WITH source AS (
    SELECT * FROM {{ source('ceva', 'pre_advices') }}
	{% if is_incremental() %}
	WHERE actual_dstamp >= "{{ get_max_partition('actual_timestamp', lookback_window=7) }}"
	{% endif %}
)

, cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
			'pre_advice_id'
			, 'po_number'
			, 'sku_id'
			]) }} as pre_advice_uuid
		, pre_advice_id
        , po_number
		, LOWER(status) AS status
		, tracking_num AS tracking_number
		, sku_id AS sku
		, description
		, qty_due
		, qty_rec
		, remaining
		, total_pa_qty_due
		, notes
		, ta_time_hrs
		, qty_rec_cs
		, qty_rec_pl
		, source_file_name
		, source_synced_at
        , 'ceva' AS provider
        , SAFE_CAST(to_date AS DATE) AS to_date
        , SAFE_CAST(from_date AS DATE) AS from_date
    	, SAFE_CAST(actual_dstamp AS TIMESTAMP) AS actual_timestamp
		, SAFE_CAST(finish_dstamp AS TIMESTAMP) AS finish_timestamp
    FROM source
)

SELECT *
FROM cleaned
