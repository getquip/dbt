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
	SELECT * FROM {{ source("littledata", "identifies") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		LOWER(context_traits_default_address_city) AS city
		,  `name` AS company
		, context_traits_default_address_country AS country
		,  IF(LOWER(context_traits_default_address_country) = 'united states', 'US', NULL) AS country_code
		,  LOWER(context_traits_default_address_country) AS country_name
		,  JSON_EXTRACT_SCALAR(PARSE_JSON(context_external_ids), '$[0].id') AS shopify_customer_id
		, first_name AS address_first_name
		, NULL AS address_id
		, last_name AS address_last_name
		, NULL AS address_list
		, `name` AS address_name
		, context_traits_phone AS address_phone
		, context_traits_address_state AS province
		, NULL AS province_code
		, context_traits_address_postal_code AS postal_code
		, anonymous_id
		, context_integration_name AS context_app_name
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_source
		, context_campaign_term
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, CONCAT('/', TRIM(context_page_path, '/')) AS context_page_path
		, context_page_referrer
		, context_page_search
		, context_page_title
		, context_page_url
		, NULL AS context_request_ip
		, SAFE_CAST(context_screen_height AS INTEGER) AS context_screen_height
		, NULL AS context_screen_inner_height
		, NULL AS context_screen_inner_width
		, SAFE_CAST(context_screen_width AS INTEGER) AS context_screen_width
		, context_google_analytics_session_id AS context_session_id
		, NULL AS context_session_start
		, context_google_analytics_client_id AS context_source_id
		, 'shopify' AS context_source_type
		, context_timezone
		, NULL AS context_topic
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, presentment_currency AS currency
		, email
		, first_name
		, COALESCE(id, CAST(_id AS STRING)) AS event_id
		, last_name
		, loaded_at
		, `description` AS note
		, purchase_count AS order_count
		, original_timestamp
		, phone
		, received_at
		, sent_at
		, NULL AS sms_marketing_consent_consent_collected_from
		, NULL AS sms_marketing_consent_consent_updated_at
		, sms_opt_in_level AS sms_marketing_consent_opt_in_level
		, sms_consent_state AS sms_marketing_consent_state
		, `state` AS identifies_state
		, tags
		, `timestamp` AS event_at
		, user_id
		, uuid_ts
		, verified_email
	FROM source
)

SELECT 
	* 
	, "littledata" AS source_name
	, 'track' as event_type
	, context_library_name = '@segment/analytics-node' AS is_server_side
	, {{ scrub_context_page_path('context_page_path') }}
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
WHERE event_at >= '2024-06-25' -- filtering for events only after migration date to remove test noise
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC ) = 1