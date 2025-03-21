WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'order_created') }}
)

, historical__littledata AS (
	SELECT * FROM {{ source("segment", "order_completed") }}
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
		, context_cart_token
		, context_checkout_token
		, context_destination_id
		, context_destination_type
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, context_request_ip
		, context_session_id
		, context_source_id
		, context_source_type
		, context_topic
		, created_at
		, currency
		, current_subtotal_price
		, current_subtotal_price_set_presentment_money_amount
		, current_subtotal_price_set_presentment_money_currency_code
		, current_subtotal_price_set_shop_money_amount
		, current_subtotal_price_set_shop_money_currency_code
		, current_total_discounts
		, current_total_discounts_set_presentment_money_amount
		, current_total_discounts_set_presentment_money_currency_code
		, current_total_discounts_set_shop_money_amount
		, current_total_discounts_set_shop_money_currency_code
		, current_total_price
		, current_total_price_set_presentment_money_amount
		, current_total_price_set_presentment_money_currency_code
		, current_total_price_set_shop_money_amount
		, current_total_price_set_shop_money_currency_code
		, current_total_tax
		, current_total_tax_set_presentment_money_amount
		, current_total_tax_set_presentment_money_currency_code
		, current_total_tax_set_shop_money_amount
		, current_total_tax_set_shop_money_currency_code
		, customer_locale
		, discount_applications
		, discount_codes
		, email
		, estimated_taxes
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
		, subtotal_price
		, subtotal_price_set_presentment_money_amount
		, subtotal_price_set_presentment_money_currency_code
		, subtotal_price_set_shop_money_amount
		, subtotal_price_set_shop_money_currency_code
		, tags
		, tax
		, tax_exempt
		, tax_lines
		, taxes_included
		, `timestamp`
		, token
		, total_discounts
		, total_discounts_set_presentment_money_amount
		, total_discounts_set_presentment_money_currency_code
		, total_discounts_set_shop_money_amount
		, total_discounts_set_shop_money_currency_code
		, total_line_items_price
		, total_line_items_price_set_presentment_money_amount
		, total_line_items_price_set_presentment_money_currency_code
		, total_line_items_price_set_shop_money_amount
		, total_line_items_price_set_shop_money_currency_code
		, total_outstanding
		, total_price_set_presentment_money_amount
		, total_price_set_presentment_money_currency_code
		, total_price_set_shop_money_amount
		, total_price_set_shop_money_currency_code
		, total_shipping_price_set_presentment_money_amount
		, total_shipping_price_set_presentment_money_currency_code
		, total_shipping_price_set_shop_money_amount
		, total_shipping_price_set_shop_money_currency_code
		, total_tax_set_presentment_money_amount
		, total_tax_set_presentment_money_currency_code
		, total_tax_set_shop_money_amount
		, total_tax_set_shop_money_currency_code
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
		, AS admin_graphql_api_id
		, anonymous_id
		, AS app_id
		, AS browser_ip
		, AS buyer_accepts_marketing
		, AS cart_token
		, checkout_id
		, AS checkout_token
		, AS client_details_accept_language
		, AS client_details_browser_ip
		, AS client_details_user_agent
		, AS closed_at
		, AS confirmation_number
		, AS is_confirmed
		, AS contact_email
		, AS context_cart_token
		, AS context_checkout_token
		, AS context_destination_id
		, AS context_destination_type
		, AS context_integration_name
		, AS context_ip
		, AS context_library_name
		, AS context_library_version
		, AS context_request_ip
		, AS context_session_id
		, AS context_source_id
		, AS context_source_type
		, AS context_topic
		, AS created_at
		, AS currency
		, AS current_subtotal_price
		, AS current_subtotal_price_set_presentment_money_amount
		, AS current_subtotal_price_set_presentment_money_currency_code
		, AS current_subtotal_price_set_shop_money_amount
		, AS current_subtotal_price_set_shop_money_currency_code
		, AS current_total_discounts
		, AS current_total_discounts_set_presentment_money_amount
		, AS current_total_discounts_set_presentment_money_currency_code
		, AS current_total_discounts_set_shop_money_amount
		, AS current_total_discounts_set_shop_money_currency_code
		, AS current_total_price
		, AS current_total_price_set_presentment_money_amount
		, AS current_total_price_set_presentment_money_currency_code
		, AS current_total_price_set_shop_money_amount
		, AS current_total_price_set_shop_money_currency_code
		, AS current_total_tax
		, AS current_total_tax_set_presentment_money_amount
		, AS current_total_tax_set_presentment_money_currency_code
		, AS current_total_tax_set_shop_money_amount
		, AS current_total_tax_set_shop_money_currency_code
		, AS customer_locale
		, AS discount_applications
		, AS discount_codes
		, AS email
		, AS estimated_taxes
		, `event` AS event_name
		, AS event_text
		, AS financial_status
		, AS fulfillment_status
		, AS fulfillments
		, AS id
		, AS landing_site
		, AS landing_site_ref
		, loaded_at
		, AS location_id
		, AS `name`
		, AS note
		, AS note_attributes
		, AS `number`
		, shopify_order_id AS order_id
		, AS order_number
		, AS order_status_url
		, original_timestamp
		, payment_gateway_littledata AS payment_gateway_names
		, AS payment_terms_created_at
		, AS payment_terms_id
		, AS payment_terms_payment_terms_name
		, AS payment_terms_payment_terms_type
		, AS payment_terms_updated_at
		, context_traits_phone AS phone
		, presentment_currency
		, AS processed_at
		, AS products
		, received_at
		, AS referring_site
		, sent_at
		, AS shipping_lines
		, AS channel
		, subtotal AS subtotal_price
		, AS subtotal_price_set_presentment_money_amount
		, AS subtotal_price_set_presentment_money_currency_code
		, AS subtotal_price_set_shop_money_amount
		, AS subtotal_price_set_shop_money_currency_code
		, AS tags
		, tax
		, AS tax_exempt
		, AS tax_lines
		, AS taxes_included
		, AS `timestamp`
		, AS token
		, AS total_discounts
		, AS total_discounts_set_presentment_money_amount
		, AS total_discounts_set_presentment_money_currency_code
		, AS total_discounts_set_shop_money_amount
		, AS total_discounts_set_shop_money_currency_code
		, AS total_line_items_price
		, AS total_line_items_price_set_presentment_money_amount
		, AS total_line_items_price_set_presentment_money_currency_code
		, AS total_line_items_price_set_shop_money_amount
		, AS total_line_items_price_set_shop_money_currency_code
		, AS total_outstanding
		, AS total_price_set_presentment_money_amount
		, AS total_price_set_presentment_money_currency_code
		, AS total_price_set_shop_money_amount
		, AS total_price_set_shop_money_currency_code
		, AS total_shipping_price_set_presentment_money_amount
		, AS total_shipping_price_set_presentment_money_currency_code
		, AS total_shipping_price_set_shop_money_amount
		, AS total_shipping_price_set_shop_money_currency_code
		, AS total_tax_set_presentment_money_amount
		, AS total_tax_set_presentment_money_currency_code
		, AS total_tax_set_shop_money_amount
		, AS total_tax_set_shop_money_currency_code
		, AS total_tip_received
		, AS total_weight
		, AS updated_at
		, user_id
		, uuid_ts
		, AS `value`
	FROM historical__littledata
)