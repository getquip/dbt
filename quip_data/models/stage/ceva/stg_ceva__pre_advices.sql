WITH source AS (
    SELECT * FROM {{ source('ceva', 'pre_advices') }}
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
