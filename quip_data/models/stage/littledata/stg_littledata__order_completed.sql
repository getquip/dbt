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
        "checkout_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source("littledata", "order_completed") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		"littledata" AS source_name
		, checkout_id
		, context_google_analytics_client_id AS admin_graphql_api_id
		, anonymous_id
		, app_id
		, context_consent_category_preferences_advertising AS buyer_accepts_marketing
		, cart_id AS cart_token
		, context_traits_email AS contact_email
		, context_integration_name
		, context_ip
		, context_library_name
		, context_library_version
		, LOWER(source_name) AS context_source_type
		, action_source AS context_topic
		, sent_at AS created_at
		, currency
		, coupon AS discount_codes
		, email
		, `event` AS event_name
		, event_text
		, 'paid' AS financial_status -- assuming these are paid as this event is 'order_completed` whereas the rudderstack event is `order_created`
		, fulfillment_status
		, loaded_at
		, original_timestamp
		, payment_gateway_littledata AS payment_gateway_names
		, context_traits_phone AS phone
		, presentment_currency
		, products
		, received_at
		, context_page_referrer AS context_page_referrer
		, sent_at
		, subtotal AS subtotal_price
		, COALESCE(CAST(tax AS NUMERIC), 0) AS tax
		, IF(TIMESTAMP_DIFF(`timestamp`, original_timestamp, DAY) > 10, original_timestamp, `timestamp`) AS event_at
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
	FROM source
)


SELECT 
	* 
	, context_library_name = '@segment/analytics-node' AS is_server_side
FROM cleaned
WHERE event_at >= '2024-06-25' -- filtering for events only after migration date to remove test noise
QUALIFY ROW_NUMBER() OVER (PARTITION BY checkout_id ORDER BY received_at DESC ) = 1
