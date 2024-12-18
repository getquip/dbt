{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"order_type", 
		"subscription_id", 
		"legacy_quip_user_id", 
		"order_id"
	]
)}}

WITH source AS (
	SELECT * FROM {{ source('quip_public', 'orders') }}
)

, renamed AS (
	SELECT
		id AS order_id
		, user_id AS legacy_quip_user_id
		, subscription_id
		
		, COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
		, _fivetran_synced AS source_synced_at

		, created_at
		, updated_at
		, IF(status IN ('cancelled', 'canceled'), updated_at, NULL) AS cancelled_at -- not always correct

		, IF(fulfillment_completed_at IS NULL, 'unfulfilled', 'fulfilled') AS  fulfillment_status
		, 'legacy_no_status' AS payment_status

		, order_type
		, weight / 16 AS weight_in_lbs

		, total_cost
		, tax_paid_by_quip
		, tax
		, subtotal
		, tax_rate

	FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted