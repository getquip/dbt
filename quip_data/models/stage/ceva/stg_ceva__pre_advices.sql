WITH source AS (
    SELECT * FROM {{ source('ceva', 'inventory_transactions') }}
)

, cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
			'pre_advice_id'
			, 'po_number'
			, 'sku'
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
        , DATE(to_date) AS to_date
        , DATE(from_date) AS from_date
        , TIMESTAMP(actual_dstamp) AS actual_timestamp
		, TIMESTAMP(finish_dstamp) AS finish_timestamp
        , LOWER(code) AS code
    FROM source
)

SELECT
FROM cleaned
