WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'tracks') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	"rudderstack" AS source_name
	, anonymous_id
	, channel
	, context_app_name
	, context_app_namespace
	, context_app_version
	, context_campaign_campaign
	, context_campaign_content
	, context_campaign_creative
	, context_campaign_device
	, context_campaign_id
	, context_campaign_medium
	, context_campaign_name
	, context_campaign_source
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
	, context_topic
	, context_user_agent
	, `event` AS event_name
	, event_text
	, id AS tracks_id
	, loaded_at
	, original_timestamp
	, received_at
	, sent_at
	, `timestamp`
	, user_id
	, uuid_ts
FROM source
