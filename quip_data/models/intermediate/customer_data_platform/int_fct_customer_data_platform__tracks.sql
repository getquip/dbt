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
        "track_event_id"
    ]
) }}

WITH

rudderstack AS (
	SELECT * FROM {{ ref('stg_rudderstack__tracks') }}
)

, legacy AS (
	SELECT * FROM {{ ref('base_customer_data_platform__legacy_sessions') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	track_event_id
	, user_id
	, anonymous_id
	, event_at
	, is_server_side
	, event_type
	, event_name
	, context_page_path
	, context_page_search
	, context_page_title
	, context_page_url
	, context_user_agent
	, context_campaign_content
	, context_campaign_medium
	, context_campaign_name
	, context_campaign_source
	, context_campaign_term
	, context_ip
	, context_locale
	, NULL AS context_campaign_type
	, NULL AS context_campaign_expid
	, context_page_referrer
	, context_campaign_id
	, context_library_name
	, context_library_version
	, context_app_version
	, context_device_manufacturer
	, context_device_type
	, NULL AS context_os_name
	, NULL AS context_os_version
	, context_screen_height
	, context_screen_width
	, source_name
FROM rudderstack

UNION ALL

SELECT
	event_id AS track_event_id
	, user_id
	, anonymous_id
	, event_at
	, is_server_side
	, event_type
	, event_name
	, context_page_path
	, context_page_search
	, context_page_title
	, context_page_url
	, context_user_agent
	, context_campaign_content
	, context_campaign_medium
	, context_campaign_name
	, context_campaign_source
	, context_campaign_term
	, context_ip
	, context_locale
	, context_campaign_type
	, context_campaign_expid
	, context_campaign_referrer AS context_page_referrer
	, context_campaign_id
	, context_library_name
	, context_library_version
	, context_app_version
	, context_device_manufacturer
	, context_device_type
	, context_os_name
	, context_os_version
	, context_screen_height
	, context_screen_width
	, source_name
FROM legacy
WHERE event_type = 'track'