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

quip_production AS (
	SELECT * FROM {{ source("legacy_segment", "quip_production__pages") }}
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
		, context_campaign_source
		, context_campaign_term
		, context_ip
		, context_locale
		, CONCAT('/', TRIM( context_page_path , '/')) AS context_page_path
		, context_page_referrer
		, context_page_search
		, context_page_title
		, context_page_url
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, context_campaign_type
		, context_campaign_expid
		, context_campaign_referrer
		, context_campaign_id
		, context_library_name
		, context_library_version
		, CAST(NULL AS STRING) AS context_app_version
		, CAST(NULL AS INTEGER) AS context_screen_height
		, CAST(NULL AS INTEGER) AS context_screen_width
    FROM quip_production
)

SELECT
	*
	, 'quip_production' AS source_name
    , 'page' AS event_type
	, FALSE AS is_server_side
	, {{ scrub_context_page_path('context_page_path') }} 
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1