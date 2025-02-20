WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'tracks') }}
)


SELECT
	id AS event_id
	, anonymous_id
	, user_id AS shopify_user_id
	, event AS event_name
	, created_at
	, updated_at
	, received_at
	, sent_at
	, processed_at
	, loaded_at AS source_synced_at
	, original_timestamp
	, timestamp AS event_timestamp
	, source_name AS event_source_name
FROM source
WHERE NOT test
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC ) = 1