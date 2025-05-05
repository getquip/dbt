WITH

littledata AS (
	SELECT * FROM {{ ref('stg_littledata__order_completed') }}
)

, legacy AS (
	SELECT * FROM {{ ref('stg_legacy_segment__order_completed') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------


	SELECT
		source_name
		, event_id
		, shopify_order_id
		, checkout_id
		, event_at
		, discount_code
		, fulfillment_status
		, payment_gateway_names
		, products
		, affiliation
	FROM littledata

	UNION ALL

	SELECT
		source_name
		, event_id
		, NULL AS shopify_order_id
		, checkout_id
		, event_at
		, NULL AS discount_code
		, NULL AS fulfillment_status
		, NULL AS payment_gateway_names
		, NULL AS products
		, NULL AS affiliation
	FROM legacy
