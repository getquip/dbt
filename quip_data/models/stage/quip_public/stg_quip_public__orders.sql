{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"order_type", 
		"legacy_subscription_id", 
		"legacy_customer_id", 
		"legacy_order_id"
	]
)}}

WITH source AS (
	SELECT * FROM {{ source('quip_public', 'orders') }}
)

, renamed AS (
	SELECT
		id AS legacy_order_id
		, user_id AS legacy_customer_id
		, subscription_id AS legacy_subscription_id
		
		, COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
		, _fivetran_synced AS source_synced_at

		, created_at
		, updated_at

		, 'legacy_no_status' AS payment_status
		, LOWER(status) AS detailed_status
		, IF(fulfillment_completed_at IS NULL, 'unfulfilled', 'fulfilled') AS  fulfillment_status
		, service_level AS fulfillment_service_level

		, LOWER(order_type) AS order_type
		, weight / 16 AS weight_in_lbs

		, total_cost
		, tax_paid_by_quip
		, tax
		, subtotal
		, tax_rate

	FROM source
)

SELECT
	*
	, CASE
		WHEN detailed_status IN (
			'ceva_cancelled'
			,'fulfillment_failed'
			,'easypost_failure'
			,'newgistics_canceled'
			,'charge_failed'
			,'canceled'
			,'recipient_credited'
			,'cx_blocked'
			) THEN 'failed_or_canceled'
		WHEN detailed_status IN (
			'easypost_unknown'
			,'preordered'
			,'ceva_allocated'
			,'ceva_available'
			,'ceva_hold'
			,'ceva_created'
			,'ceva_in_progress'
			,'ceva_packed'
			,'ceva_picked'
			,'ceva_ready_to_load'
			,'ceva_released'
			,'easypost_pre_transit'
			,'hold'
			,'fulfillment_queued'
			,'fulfillment_requesting'
			,'fulfillment_requested'
			,'newgistics_inv_hold'
			,'newgistics_on_hold'
			,'newgistics_printed'
			,'newgistics_received'
			,'newgistics_updated'
			,'newgistics_verified'
			,'pending_charge'
			,'waiting'
			,'unfulfilled'
			,'wholesale_unfulfilled'
			, 'temp_review_hold'
			, 'review_hold'
			, 'denmat_started'
			, 'denmat_partially_on_hold'
			, 'denmat_on_hold'
			) THEN 'pending'
		WHEN detailed_status IN (
			'ceva_shipped'
			,'easypost_in_transit'
			,'easypost_out_for_delivery'
			,'newgistics_shipped'
			) THEN 'in_transit'
		WHEN detailed_status IN (
			 'easypost_delivered'
			 ,'fulfillment_completed'
			 ,'newgistics_delivered'
			 ,'ceva_complete'
			 ) THEN 'delivered'
		WHEN detailed_status IN (
			'easypost_delivered_to_sender'
			,'easypost_return_to_sender'
			,'newgistics_badaddress'
			,'newgistics_returned'
			,'address_error'
			) THEN 'return_to_sender'
		WHEN detailed_status = 'subscription_activated' THEN 'subscription_activated'
		ELSE 'no_mapped_order_status_need_to_fix'
		END AS order_status
	, IF(detailed_status IN ('cancelled', 'canceled'), updated_at, NULL) AS cancelled_at -- not always correct
FROM renamed
WHERE NOT is_source_deleted