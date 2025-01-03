WITH source AS (
	SELECT * FROM {{ source('shopify', 'fulfillment_event') }}
)

, renamed AS (
	SELECT
		id AS shopify_fulfillment_event_id
		, COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
		, _fivetran_synced AS source_synced_at
		, created_at
		, updated_at
		, fulfillment_id AS shopify_fulfillment_id
		, happened_at AS event_at
		, order_id AS shopify_order_id
		, status AS order_status
	FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted