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
        "page_event_id"
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
		"littledata" AS source_name
		, anonymous_id
		, NULL AS page_category
		, context_campaign_capaign AS context_campaign_campaign
		, context_campaign_clickid
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_expid
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, NULL AS context_campaign_referrer
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
		, NULL AS context_source_id
		, NULL AS context_source_type
		, context_timezone
		, context_user_agent
		, id AS page_event_id
		, NULL AS initial_referrer
		, NULL AS initial_referring_domain
		, loaded_at
		, `name` AS page_name
		, original_timestamp
		, `path` AS page_path
		, received_at
		, referrer
		, NULL AS referring_domain
		, search
		, sent_at
		, NULL AS tab_url
		, `timestamp`
		, title
		, `url` AS page_url
		, user_id
		, uuid_ts
	FROM source
)

SELECT 
	* 
	, 'page' as event_type
	, IF(context_library_name = '@segment/analytics-node', 'backend', 'web') AS platform
	--this removes any unique identifiers from the page path
	, {{ scrub_context_page_path(context_page_path) }}
	, {{ create_touchpoint('context_page_path') }}
FROM cleaned
-- filtering for events only after migration date to remove test noise
WHERE `timestamp` >= '2024-06-25'
QUALIFY ROW_NUMBER() OVER (PARTITION BY page_event_id ORDER BY received_at DESC ) = 1
