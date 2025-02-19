WITH

source AS (
	SELECT * FROM {{ source('oxford_road', 'podcast') }}
) 

, cleaned AS (
	SELECT
		TRIM(ad_unit) AS ad_id
		, LOWER(spot_type) AS spot_type
		, IF(LENGTH(SPLIT(start_date, "/")[2]) = 4, PARSE_DATE("%m/%d/%Y", start_date), PARSE_DATE("%m/%d/%y", start_date)) AS date
		, 'podcast' as marketing_channel
		, 'audio' as marketing_channel_grouping
		, 'podcast' as marketing_platform
		, network as vendor
		, 'national' as DMA
		, 'consumer' as target_audience
		, 'direct response' as campaign_type
		, COALESCE(utm_source, "NA") as utm_campaign_source
		, COALESCE(utm_medium, "NA") as utm_campaign_medium
		, COALESCE(utm_campaign, "NA") as utm_campaign_name 
		, SAFE_CAST(REPLACE(impressions, ',', '') AS INTEGER) AS impressions
		, SAFE_CAST(REPLACE(client_net, '$', '') AS FLOAT64) AS total_spend
		, SAFE_CAST(REPLACE(station_net, '$', '') AS FLOAT64) AS media_spend
		, SAFE_CAST(REPLACE(agency_comm, '$', '') AS FLOAT64) AS agency_fee
		, "performance" as campaign_objective
		, "ecomm" as campaign_category
	FROM source
)

SELECT * FROM cleaned