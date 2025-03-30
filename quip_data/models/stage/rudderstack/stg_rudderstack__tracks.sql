{{ config(
    materialized='incremental',
	incremental_strategy='merge',
	unique_key='track_event_id',
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

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'tracks') }}
	{% if is_incremental() %}
		WHERE received_at >= "{{ get_max_partition('received_at') }}"
	{% endif %}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		anonymous_id
		, channel
		, context_app_name
		, context_app_namespace
		, context_app_version
		, context_campaign_campaign
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_id
		, LOWER(context_campaign_medium) AS context_campaign_medium
		, context_campaign_name
		, LOWER(context_campaign_source) AS context_campaign_source
		, context_campaign_term
		, context_cart_token
		, context_checkout_token
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, context_order_token
		, context_page_initial_referrer
		, context_page_initial_referring_domain
    	, CONCAT('/', TRIM(context_page_path , '/')) AS context_page_path
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
		, context_topic
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, `event` AS event_name
		, event_text
		, id AS track_event_id
		, loaded_at
		, original_timestamp
		, received_at
		, sent_at
		, `timestamp` AS event_at
		, user_id
		, uuid_ts
	FROM source
)

, parsed AS (
	SELECT 
		* 
		, context_library_name != 'RudderLabs JavaScript SDK' AS is_server_side
		, {{ scrub_context_page_path('context_page_path') }}
		, {{ parse_device_info_from_user_agent('device_info') }}
	FROM cleaned
)

SELECT
	* EXCEPT(context_campaign_device, context_device_type)
	, "rudderstack" AS source_name
	, 'track' as event_type	
	, COALESCE(
		CASE
			WHEN context_campaign_device = 'c' THEN 'computer/desktop'
			WHEN context_campaign_device = 'm' THEN 'mobile'
			WHEN context_campaign_device = 't' THEN 'tablet'
		END 
		, context_device_type
	) AS context_device_type
FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY track_event_id ORDER BY received_at DESC ) = 1
