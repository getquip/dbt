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

tracks AS (
	SELECT * FROM {{ ref('stg_rudderstack__tracks') }}
)

, pages AS (
	SELECT * FROM {{ ref('stg_rudderstack__pages') }}
)

, legacy AS (
	SELECT * FROM {{ ref('base_customer_data_platform__legacy_sessions') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, events AS (
	SELECT
		track_event_id AS event_id
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
	FROM tracks

	UNION ALL

	SELECT
		page_event_id AS event_id
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
	FROM pages

	UNION ALL

	SELECT
		event_id
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
	WHERE event_type IN ('track', 'page', 'screen')
)


, page_event_sequence AS (
	SELECT
		events.event_id
		, events.session_id
		, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS page_event_sequence
		, TIMESTAMP_DIFF(LEAD(event_at) OVER 
			(PARTITION BY session_id, context_page_path_scrubbed ORDER BY event_at), event_at, SECOND) AS page_time_spent_seconds
	FROM events
	WHERE event_type IN ('page', 'screen')
)

, track_event_sequence AS (
	SELECT
		events.event_id
		, events.session_id
		, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS track_event_sequence
	FROM events
	WHERE event_type = 'track'
)

SELECT
	events.*
	, pages.page_event_sequence
	, pages.page_time_spent_seconds
	, tracks.track_event_sequence
	, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS event_sequence
FROM events
LEFT JOIN page_event_sequence AS pages
	ON events.event_id = pages.event_id
LEFT JOIN track_event_sequence AS tracks
	ON events.event_id = tracks.event_id
WHERE event_type IN ('page', 'screen', 'track')