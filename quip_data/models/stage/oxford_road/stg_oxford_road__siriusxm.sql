WITH

source AS (
	SELECT * FROM {{ source('oxford_road', 'siriusxm') }}
) 

, cleaned AS (
	SELECT
		TRIM(ad_id) AS ad_id
		, IF(LENGTH(SPLIT(date, "/")[2]) = 4, PARSE_DATE("%m/%d/%Y", date), PARSE_DATE("%m/%d/%y", date)) AS date
		, 'audio' as marketing_channel
		, 'audio' as marketing_channel_grouping
		, 'sirius xm' as marketing_platform
		, channels_placed_ as vendor
		, 'national' as DMA
		, 'consumer' as target_audience
		, 'direct response' as campaign_type
		, COALESCE(utm_source, "NA") as utm_campaign_source
		, COALESCE(utm_medium, "NA") as utm_campaign_medium
		, COALESCE(utm_campaign, "NA") as utm_campaign_name 
		, SAFE_CAST(REPLACE(client_net, '$', '') AS FLOAT64) AS total_spend
		, SAFE_CAST(REPLACE(station_net, '$', '') AS FLOAT64) AS media_spend
		, SAFE_CAST(REPLACE(agency_comm, '$', '') AS FLOAT64) AS agency_fee
		, "performance" as campaign_objective
		, "ecomm" as campaign_category
	FROM source
)

SELECT * FROM cleaned