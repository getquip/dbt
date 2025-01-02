WITH source AS (
	  SELECT * FROM {{ source('quip_public', 'subscriptions') }}
)

, renamed AS (
	SELECT
		_fivetran_deleted AS is_source_deleted
		, _fivetran_synced AS source_synced_at

		, id AS legacy_quip_subscription_id
		, user_id AS legacy_quip_user_id
		, order_id AS latest_order_id
		, SAFE_CAST(created_at AS TIMESTAMP) AS created_at
		, SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
		, status
	FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted