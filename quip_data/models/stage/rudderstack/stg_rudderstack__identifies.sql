{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "identifies_event_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'identifies') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
		SELECT
		"rudderstack" AS source_name
		, address_address1 AS address1
		, address_address2 AS address2
		, LOWER(address_city) AS city
		, address_company AS company
		, address_country AS country
		, address_country_code AS country_code
		, address_country_name AS country_name
		, CAST(address_customer_id AS STRING) AS customer_id
		, address_default
		, address_first_name
		, address_id
		, address_last_name
		, address_list
		, address_name
		, address_phone
		, address_province AS province
		, address_province_code AS province_code
		, address_zip AS postal_code
		, admin_graphql_api_id
		, anonymous_id
		, context_app_name
		, context_app_namespace
		, context_app_version
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_id
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_source
		, context_campaign_term
		, context_destination_id
		, context_destination_type
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, context_page_path
		, COALESCE(context_page_referrer, context_page_initial_referrer) AS context_page_referrer
		, COALESCE(context_page_referring_domain, context_page_initial_referring_domain) AS context_page_referring_domain
		, context_page_search
		, context_page_title
		, context_page_url
		, context_request_ip
		, context_screen_height
		, context_screen_width
		, CAST(context_session_id AS STRING) AS session_id
		, context_session_start
		, context_source_id
		, LOWER(context_source_type) AS context_source_type
		, context_timezone
		, context_topic
		, context_user_agent
		, LOWER(context_user_agent) AS device_info
		, currency
		, email
		, first_name
		, id AS identifies_event_id
		, last_name
		, loaded_at
		, note
		, order_count
		, original_timestamp
		, phone
		, received_at
		, sent_at
		, sms_marketing_consent_consent_collected_from
		, sms_marketing_consent_consent_updated_at
		, sms_marketing_consent_opt_in_level
		, sms_marketing_consent_state
		, `state` AS identifies_state
		, tags
		, tax_exempt
		, `timestamp` AS event_at
		, total_spent
		, user_id
		, uuid_ts
		, verified_email
	FROM source
)


SELECT
	*
	, context_library_name != 'RudderLabs JavaScript SDK' AS is_server_side
	, {{ scrub_context_page_path('context_page_path') }}
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY identifies_event_id ORDER BY received_at DESC ) = 1