WITH

source AS (
	SELECT * FROM {{ source("wen_parker", "hts_duties") }}
)

SELECT
	_fivetran_synced AS source_synced_at
	, REGEXP_REPLACE(code, r'\D', '') AS sku
	, LOWER(item) AS sku_name
	, LOWER(`group`) AS sku_group
	, LOWER(description) AS sku_description
	, value_per_unit
	, hts AS tariff_number 
	, duty_rate_ AS duty_rate
	, add_t_hts_cn AS additional_hts_china
	, hts_cn_duty_rate_ AS china_duty_rate
	, SAFE_CAST(IF(LOWER(fda_required) = 'yes', 1, 0) AS BOOLEAN) AS is_fda_required
	, mpf AS merchandise_processing_fee
	, harbor_maintenance
	, SAFE_CAST(weight_lb_ AS NUMERIC) AS weight_lb
FROM source