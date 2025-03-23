WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'order_created') }}
)

, historical__littledata AS (
	SELECT * FROM {{ source("segment", "littledata__order_completed") }}
)

, historical__legacy AS (
	SELECT * FROM {{ source("segment", "legacy__order_completed") }}
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
		, closed_at
		, confirmation_number
		, confirmed AS is_confirmed
		, contact_email
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_request_ip
		, context_session_id
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
		, `timestamp`
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

	UNION ALL

	SELECT
		"littledata" AS source_name
		, context_google_analytics_client_id AS admin_graphql_api_id
		, anonymous_id
		, app_id
		, NULL AS browser_ip
		, context_consent_category_preferences_advertising AS buyer_accepts_marketing
		, cart_id AS cart_token
		, checkout_id
		, NULL AS checkout_token
		, NULL AS client_details_accept_language
		, NULL AS client_details_browser_ip
		, NULL AS client_details_user_agent
		, NULL AS closed_at
		, NULL AS confirmation_number
		, NULL AS is_confirmed
		, context_traits_email AS contact_email
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, NULL AS context_request_ip
		, context_google_analytics_session_id AS context_session_id
		, NULL AS context_source_id
		, LOWER(source_name) AS context_source_type
		, action_source AS context_topic
		, sent_at AS created_at
		, currency
		, NULL AS customer_locale
		, NULL AS discount_applications
		, coupon AS discount_codes
		, email
		, NULL AS has_estimated_taxes
		, `event` AS event_name
		, event_text
		, 'paid' AS financial_status -- assuming these are paid as this event is 'order_completed` whereas the rudderstack event is `order_created`
		, fulfillment_status
		, NULL AS fulfillments
		, NULL AS order_created_id
		, NULL AS landing_site
		, NULL AS landing_site_ref
		, loaded_at
		, NULL AS location_id
		, NULL AS `name`
		, NULL AS note
		, NULL AS note_attributes
		, NULL AS `number`
		, NULL AS order_id
		, NULL AS order_number
		, NULL AS order_status_url
		, original_timestamp
		, payment_gateway_littledata AS payment_gateway_names
		, NULL AS payment_terms_created_at
		, NULL AS payment_terms_id
		, NULL AS payment_terms_payment_terms_name
		, NULL AS payment_terms_payment_terms_type
		, NULL AS payment_terms_updated_at
		, context_traits_phone AS phone
		, presentment_currency
		, NULL AS processed_at
		, products
		, received_at
		, context_page_referrer AS referring_site
		, sent_at
		, NULL AS shipping_lines
		, NULL AS channel
		, subtotal AS subtotal_price
		, NULL AS tags
		, COALESCE(CAST(tax AS NUMERIC), 0) AS tax
		, NULL AS tax_exempt
		, NULL AS tax_lines
		, NULL AS taxes_included
		, `timestamp`
		, NULL AS token
		, COALESCE(CAST(discount AS NUMERIC), 0) AS total_discounts
		, COALESCE(CAST(total AS NUMERIC), 0) AS total_line_items_price
		, NULL AS total_outstanding
		, NULL AS total_price_set_shop_money_amount
		, CAST(shipping AS NUMERIC) AS total_shipping_price_set_presentment_money_amount
		, NULL AS total_tax_set_presentment_money_amount
		, NULL AS total_tax_set_shop_money_amount
		, NULL AS total_tip_received
		, NULL AS total_weight
		, loaded_at AS updated_at
		, user_id
		, uuid_ts
		, CAST(purchase_count_littledata AS STRING) AS `value`
	FROM historical__littledata

	UNION ALL

	SELECT
		"legacy" AS source_name
		, NULL AS admin_graphql_api_id
		, anonymous_id
		, NULL AS app_id
		, NULL AS browser_ip
		, NULL AS buyer_accepts_marketing
		, NULL AS cart_token
		, NULL AS checkout_id
		, token AS checkout_token
		, NULL AS client_details_accept_language
		, NULL AS client_details_browser_ip
		, NULL AS client_details_user_agent
		, NULL AS closed_at
		, NULL AS confirmation_number
		, NULL AS is_confirmed
		, email AS contact_email
		, NULL AS context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, NULL AS context_request_ip
		, NULL AS context_session_id
		, NULL AS context_source_id
		, `type` AS context_source_type
		, NULL AS context_topic
		, NULL AS created_at
		, NULL AS currency
		, NULL AS customer_locale
		, NULL AS discount_applications
		, coupon AS discount_codes
		, email
		, NULL AS has_estimated_taxes
		, `event` AS event_name
		, event_text
		, NULL AS financial_status
		, NULL AS fulfillment_status
		, NULL AS fulfillments
		, id AS order_created_id
		, NULL AS landing_site
		, NULL AS landing_site_ref
		, loaded_at
		, NULL AS location_id
		, NULL AS `name`
		, NULL AS note
		, NULL AS note_attributes
		, NULL AS `number`
		, order_id
		, NULL AS order_number
		, NULL AS order_status_url
		, original_timestamp
		, NULL AS payment_gateway_names
		, NULL AS payment_terms_created_at
		, NULL AS payment_terms_id
		, NULL AS payment_terms_payment_terms_name
		, NULL AS payment_terms_payment_terms_type
		, NULL AS payment_terms_updated_at
		, phone
		, NULL AS presentment_currency
		, NULL AS processed_at
		, products
		, received_at
		, NULL AS referring_site
		, sent_at
		, shipping_name AS shipping_lines
		, NULL AS channel
		, subtotal AS subtotal_price
		, NULL AS tags
		, COALESCE(CAST(tax AS NUMERIC), 0) AS tax
		, NULL AS tax_exempt
		, NULL AS tax_lines
		, NULL AS taxes_included
		, `timestamp`
		, token
		, COALESCE(CAST(discount AS NUMERIC), 0) AS total_discounts
		, COALESCE(CAST(total AS NUMERIC), 0) AS total_line_items_price
		, NULL AS total_outstanding
		, NULL AS total_price_set_shop_money_amount
		, CAST(shipping AS NUMERIC) AS total_shipping_price_set_presentment_money_amount
		, NULL AS total_tax_set_presentment_money_amount
		, NULL AS total_tax_set_shop_money_amount
		, NULL AS total_tip_received
		, NULL AS total_weight
		, NULL AS updated_at
		, user_id
		, uuid_ts
		, NULL AS `value`
	FROM historical__legacy
)

SELECT * FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_created_id ORDER BY loaded_at DESC) = 1

