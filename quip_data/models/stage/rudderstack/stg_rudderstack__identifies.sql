{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "identifies_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'identifies') }}
)

, historical__littledata AS (
	SELECT * FROM {{ source("segment", "littledata__identifies") }}
)

, historical__legacy AS (
	SELECT * FROM {{ source("segment", "legacy__identifies") }}
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
		, channel
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
		, context_screen_density
		, context_screen_height
		, context_screen_inner_height
		, context_screen_inner_width
		, context_screen_width
		, CAST(context_session_id AS STRING) AS context_session_id
		, context_session_start
		, context_source_id
		, LOWER(context_source_type) AS context_source_type
		, context_timezone
		, context_topic
		, context_user_agent
		, currency
		, email
		, first_name
		, id AS identifies_id
		, last_name
		, last_order_name
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
		, state AS identifies_state
		, tags
		, tax_exempt
		, timestamp
		, total_spent
		, user_id
		, uuid_ts
		, verified_email
	FROM source

	{% if not is_incremental() %}
		UNION ALL
		
		SELECT
			"littledata" AS source_name
			, context_traits_default_address_street AS address1
			,  NULL AS address2
			, LOWER(context_traits_default_address_city) AS city
			,  `name` AS company
			, context_traits_default_address_country AS country
			,  IF(LOWER(context_traits_default_address_country) = 'united states', 'US', NULL) AS country_code
			,  NULL AS country_name
			,  JSON_EXTRACT_SCALAR(PARSE_JSON(context_external_ids), '$[0].id') AS customer_id
			, context_traits_default_address_street = default_address_street AS address_default
			, first_name AS address_first_name
			, NULL AS address_id
			, last_name AS address_last_name
			, NULL AS address_list
			, `name` AS address_name
			, context_traits_phone AS address_phone
			, context_traits_address_state AS province
			, NULL AS province_code
			, context_traits_address_postal_code AS postal_code
			, NULL AS admin_graphql_api_id
			, anonymous_id
			, IF(context_library_name = 'analytics.js', 'web', NULL) AS channel
			, context_integration_name AS context_app_name
			, NULL AS context_app_namespace
			, NULL AS context_app_version
			, context_campaign_content
			, context_campaign_creative
			, context_campaign_device
			, context_campaign_id
			, context_campaign_medium
			, context_campaign_name
			, context_campaign_source
			, context_campaign_term
			, NULL AS context_destination_id
			, NULL AS context_destination_type
			, context_integration_name
			, context_ip
			, context_library_name
			, context_library_version
			, context_locale
			, context_page_referrer AS context_page_initial_referrer
			, NULL AS context_page_initial_referring_domain
			, context_page_path
			, context_page_referrer
			, NULL AS context_page_referring_domain
			, context_page_search
			, NULL AS context_page_tab_url
			, context_page_title
			, context_page_url
			, NULL AS context_request_ip
			, NULL AS context_screen_density
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
			, presentment_currency AS currency
			, email
			, first_name
			, COALESCE(id, CAST(_id AS STRING)) AS identifies_id
			, last_name
			, NULL AS last_order_name
			, loaded_at
			, description AS note
			, purchase_count AS order_count
			, original_timestamp
			, phone
			, received_at
			, sent_at
			, NULL AS sms_marketing_consent_consent_collected_from
			, NULL AS sms_marketing_consent_consent_updated_at
			, sms_opt_in_level AS sms_marketing_consent_opt_in_level
			, sms_consent_state AS sms_marketing_consent_state
			, state AS identifies_state
			, tags
			, NULL AS tax_exempt
			, timestamp
			, NULL AS total_spent
			, user_id
			, uuid_ts
			, verified_email
		FROM historical__littledata

		UNION ALL

			SELECT
				"legacy" AS source_name
				, NULL AS address1
				, NULL AS address2
				, NULL AS city
				, NULL AS company
				, NULL AS country
				, NULL AS country_code
				, NULL AS country_name
				, NULL AS customer_id
				, NULL AS address_default
				, `name` AS address_first_name
				, NULL AS address_id
				, `name` AS address_last_name
				, NULL AS address_list
				, `name` AS address_name
				, NULL AS address_phone
				, NULL AS province
				, NULL AS province_code
				, NULL AS postal_code
				, NULL AS admin_graphql_api_id
				, anonymous_id
				, NULL AS channel
				, NULL AS context_app_name
				, NULL AS context_app_namespace
				, NULL AS context_app_version
				, context_campaign_content
				, context_campaign_creative
				, context_campaign_device
				, NULL AS context_campaign_id
				, context_campaign_medium
				, context_campaign_name
				, context_campaign_source
				, context_campaign_term
				, NULL AS context_destination_id
				, NULL AS context_destination_type
				, NULL AS context_integration_name
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
				, NULL AS context_screen_density
				, NULL AS context_screen_height
				, NULL AS context_screen_inner_height
				, NULL AS context_screen_inner_width
				, NULL AS context_screen_width
				, NULL AS context_session_id
				, NULL AS context_session_start
				, NULL AS context_source_id
				, NULL AS context_source_type
				, context_timezone
				, NULL AS context_topic
				, context_user_agent
				, NULL AS currency
				, email
				, `name` AS first_name
				, id AS identifies_id
				, `name` AS last_name
				, NULL AS last_order_name
				, loaded_at
				, NULL AS note
				, NULL AS order_count
				, original_timestamp
				, NULL AS phone
				, received_at
				, sent_at
				, NULL AS sms_marketing_consent_consent_collected_from
				, NULL AS sms_marketing_consent_consent_updated_at
				, NULL AS sms_marketing_consent_opt_in_level
				, NULL AS sms_marketing_consent_state
				, NULL AS identifies_state
				, context_campaign_tags AS tags
				, NULL AS tax_exempt
				, timestamp
				, NULL AS total_spent
				, user_id
				, uuid_ts
				, NULL AS verified_email
		FROM historical__legacy
	{% endif %}
)


SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY identifies_id ORDER BY received_at DESC ) = 1