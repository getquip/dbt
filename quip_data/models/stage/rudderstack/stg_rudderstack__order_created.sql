{{ config(
    materialized='incremental',
	incremental_strategy='merge',
	unique_key='order_created_id',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "order_created_id"
    ]
) }}

WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'order_created') }}
	{% if is_incremental() %}
		WHERE received_at >= "{{ get_max_partition('received_at') }}"
	{% endif %}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		"rudderstack" AS source_name
		, admin_graphql_api_id
		, anonymous_id
		, app_id
		, browser_ip
		, buyer_accepts_marketing
		, cart_token
		, checkout_id
		, checkout_token
		, client_details_accept_language
		, client_details_browser_ip
		, client_details_user_agent
		, LOWER(client_details_user_agent) AS device_info
		, closed_at
		, confirmation_number
		, confirmed AS is_confirmed
		, contact_email
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_request_ip
		, CAST(context_session_id AS STRING) AS session_id
		, context_source_id
		, LOWER(context_source_type) AS context_source_type
		, context_topic
		, created_at
		, currency
		, customer_locale
		, discount_applications
		, discount_codes
		, email
		, estimated_taxes AS has_estimated_taxes
		, `event` AS event_name
		, event_text
		, financial_status
		, fulfillment_status
		, fulfillments
		, id AS order_created_id
		, landing_site
		, landing_site_ref
		, loaded_at
		, location_id
		, `name`
		, note
		, note_attributes
		, `number`
		, order_id
		, order_number
		, order_status_url
		, original_timestamp
		, payment_gateway_names
		, payment_terms_created_at
		, payment_terms_id
		, payment_terms_payment_terms_name
		, payment_terms_payment_terms_type
		, payment_terms_updated_at
		, phone
		, presentment_currency
		, processed_at
		, products
		, received_at
		, referring_site
		, sent_at
		, shipping_lines
		, source_name AS channel
		, SAFE_CAST(subtotal_price AS NUMERIC) AS subtotal_price
		, tags
		, SAFE_CAST(tax AS NUMERIC) AS tax
		, tax_exempt
		, tax_lines
		, taxes_included
		, `timestamp` AS event_at
		, token
		, COALESCE(SAFE_CAST(total_discounts AS NUMERIC), 0) AS total_discounts
		, COALESCE(SAFE_CAST(total_line_items_price AS NUMERIC), 0) AS total_line_items_price
		, total_outstanding
		, total_price_set_shop_money_amount
		, SAFE_CAST(total_shipping_price_set_presentment_money_amount AS NUMERIC) AS total_shipping_price_set_presentment_money_amount
		, total_tax_set_presentment_money_amount
		, total_tax_set_shop_money_amount
		, total_tip_received
		, total_weight
		, updated_at
		, user_id
		, uuid_ts
		, `value`
	FROM source
	WHERE NOT test
)

SELECT 
	* 
	, context_library_name != 'RudderLabs JavaScript SDK' AS is_server_side
	, {{ parse_device_info_from_user_agent('device_info') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_created_id ORDER BY loaded_at DESC) = 1

