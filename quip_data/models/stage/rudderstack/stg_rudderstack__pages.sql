WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'pages') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
,cleaned AS (

	SELECT
		"rudderstack" AS source_name
		, anonymous_id
		, category AS page_category
		, channel
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
		, CAST(context_session_id AS STRING) AS context_session_id
		, context_session_start
		, context_source_id
		, context_source_type
		, context_timezone
		, context_user_agent
		, id AS page_event_id
		, initial_referrer
		, initial_referring_domain
		, loaded_at
		, `name` AS page_name
		, original_timestamp
		, `path` AS page_path
		, received_at
		, referrer
		, referring_domain
		, search
		, sent_at
		, tab_url
		, `timestamp`
		, title
		, `url` AS page_url
		, user_id
		, uuid_ts
	FROM source

	UNION ALL



	UNION ALL

	SELECT
		"legacy" AS source_name
		, anonymous_id
		, NULL AS page_category
		, NULL AS channel
		, NULL AS context_app_name
		, NULL AS context_app_namespace
		, NULL AS context_app_version
		, context_campaign_capaign AS context_campaign_campaign
		, NULL AS context_campaign_clickid
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_expid
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_referrer
		, LOWER(COALESCE(context_campaign_sourcef, context_campaign_source, context_campaign_sougmrce)) AS context_campaign_source
		, context_campaign_term
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, NULL AS context_page_initial_referrer
		, NULL AS context_page_initial_referring_domain
		, context_page_path
		, context_page_referrer
		, NULL AS context_page_referring_domain
		, context_page_search
		, NULL AS context_page_tab_url
		, context_page_title
		, context_page_url
		, NULL AS context_request_ip
		, NULL AS context_screen_height
		, NULL AS context_screen_width
		, NULL AS context_session_id
		, NULL AS context_session_start
		, NULL AS context_source_id
		, NULL AS context_source_type
		, context_timezone
		, context_user_agent
		, id AS page_event_id
		, context_referrer_id AS initial_referrer
		, NULL AS initial_referring_domain
		, loaded_at
		, NULL AS page_name
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
	FROM historical__legacy
)

SELECT 
	* 
	, {{ scrub_context_page_path('context_page_path') }} 
	, {{ create_touchpoint('context_page_path') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY page_event_id ORDER BY loaded_at DESC) = 1