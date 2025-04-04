-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-refresh
{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "event_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source("littledata", "tracks") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
    	id AS event_id
		, user_id
    	, anonymous_id
    	, `timestamp` AS event_at
		, received_at
    	, context_campaign_content
    	, context_campaign_medium
    	, context_campaign_name
    	, LOWER(context_campaign_source) AS context_campaign_source
    	, context_campaign_term
    	, context_ip
    	, context_locale
    	, CONCAT('/', TRIM(context_page_path , '/')) AS context_page_path
    	, context_page_search
    	, context_page_title
    	, context_page_url
    	, context_user_agent
		, LOWER(context_user_agent) AS device_info
    	, context_campaign_expid
    	, COALESCE(context_campaign_referrer, context_page_referrer) AS context_page_referrer
    	, context_library_name
    	, context_library_version
    	, context_os_version AS context_app_version
    	, context_os_name AS context_os_name_v1
    	, context_os_version AS context_os_version_v1
    	, SAFE_CAST(context_screen_height AS INTEGER) AS context_screen_height
    	, SAFE_CAST(context_screen_width AS INTEGER) AS context_screen_width
	FROM source
    
)

, parsed AS (
	SELECT 
		* 
		, {{ scrub_context_page_path('context_page_path') }}
		, {{ parse_device_info_from_user_agent('device_info') }}
	FROM cleaned
)

SELECT
	* EXCEPT(context_os_name, context_os_name_v1, context_os_version, context_os_version_v1)
	, 'littledata' AS source_name
	, 'track' as event_type
	, context_library_name = '@segment/analytics-node' AS is_server_side
	, COALESCE(context_os_name, context_os_name_v1) AS context_os_name
	, COALESCE(context_os_version, context_os_version_v1) AS context_os_version
FROM parsed
 -- filtering for events only after migration date to remove test noise
WHERE event_at >= '2024-06-25'
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC ) = 1