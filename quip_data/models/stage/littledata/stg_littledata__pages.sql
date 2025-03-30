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
	SELECT * FROM {{ source("littledata", "pages") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		id AS event_id
		, anonymous_id
		, context_campaign_capaign AS context_campaign_campaign
		, context_campaign_clickid
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_expid
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_source
		, context_campaign_term
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, CONCAT('/', TRIM(context_page_path, '/')) AS context_page_path
		, context_page_referrer
		, NULL AS context_page_referring_domain
		, context_page_search
		, NULL AS context_page_tab_url
		, context_page_title
		, context_page_url
		, NULL AS context_request_ip
		, CAST(NULL AS INTEGER) AS context_screen_height
		, CAST(NULL AS INTEGER) AS context_screen_width
		, context_google_analytics_session_id AS context_session_id
		, NULL AS context_session_start
		, context_timezone
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, `name` AS event_name
		, original_timestamp
		, loaded_at
		, received_at
		, `path` AS page_path
		, referrer
		, NULL AS referring_domain
		, search
		, sent_at
		, NULL AS tab_url
		, `timestamp` AS event_at
		, title
		, `url` AS page_url
		, user_id
		, uuid_ts
	FROM source
)

SELECT 
	* 
	, "littledata" AS source_name
	, 'page' as event_type
	, context_library_name = '@segment/analytics-node' AS is_server_side
	, {{ scrub_context_page_path('context_page_path') }}
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
-- filtering for events only after migration date to remove test noise
WHERE event_at >= '2024-06-25'
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC ) = 1
