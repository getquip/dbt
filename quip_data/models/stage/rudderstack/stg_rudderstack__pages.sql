{{ config(
    materialized='incremental',
	incremental_strategy='merge',
	unique_key='page_event_id',
    partition_by={
        "field": "event_at",
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
	SELECT * FROM {{ source('rudderstack_prod', 'pages') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
,cleaned AS (

	SELECT
		id AS page_event_id
		, anonymous_id
		, category AS page_category
		, context_app_name
		, context_app_namespace
		, context_app_version
		, context_campaign_campaign
		, context_campaign_clickid
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_expid
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_referrer
		, context_campaign_source
		, context_campaign_term
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, context_page_initial_referrer
		, context_page_initial_referring_domain
		, context_page_path
		, context_page_referrer
		, context_page_referring_domain
		, context_page_search
		, context_page_tab_url
		, context_page_title
		, context_page_url
		, context_request_ip
		, context_screen_height
		, context_screen_width
		, CAST(context_session_id AS STRING) AS session_id
		, context_session_start
		, context_source_id
		, context_source_type
		, context_timezone
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, initial_referrer
		, initial_referring_domain
		, loaded_at
		, `name` AS event_name
		, original_timestamp
		, `path` AS page_path
		, received_at
		, referrer
		, referring_domain
		, search
		, sent_at
		, tab_url
		, `timestamp` AS event_at
		, title
		, `url` AS page_url
		, user_id
		, uuid_ts
		, channel
	FROM source
)

SELECT 
	* 
	, "rudderstack" AS source_name
	, 'page' AS event_type
	, context_library_name != 'RudderLabs JavaScript SDK' AS is_server_side
	, {{ scrub_context_page_path('context_page_path') }} 
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY page_event_id ORDER BY loaded_at DESC) = 1