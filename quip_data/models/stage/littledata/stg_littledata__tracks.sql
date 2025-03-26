-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-refresh
{{ config(
    materialized='table',
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "identifies_id"
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
		'littledata' AS source_name
		, user_id
    	, anonymous_id
    	, `timestamp`
    	, id AS track_event_id
    	, context_campaign_content
    	, context_campaign_medium
    	, context_campaign_name
    	, context_campaign_source
    	, context_campaign_term
    	, context_ip
    	, context_locale
    	, CONCAT('/', TRIM(t.context_page_path , '/')) AS context_page_path
    	, context_page_referrer
    	, context_page_search
    	, context_page_title
    	, context_page_url
    	, context_user_agent
    	, context_campaign_expid
    	, context_campaign_referrer
    	, context_library_name
    	, context_library_version
    	, context_os_version AS context_app_version
    	, context_os_name
    	, context_os_version
    	, SAFE_CAST(context_screen_height AS INTEGER) AS context_screen_height
    	, SAFE_CAST(context_screen_width AS INTEGER) AS context_screen_width
	FROM source
    
)

SELECT 
	* 
	, 'track' as event_type
	, IF(context_library_name = '@segment/analytics-node', 'backend', 'web') AS platform
	, {{ scrub_context_page_path('context_page_path') }} 
	, {{ create_touchpoint('context_page_path') }}
FROM cleaned
 -- filtering for events only after migration date to remove test noise
WHERE `timestamp` >= '2024-06-25'
QUALIFY ROW_NUMBER() OVER (PARTITION BY track_event_id ORDER BY received_at DESC ) = 1