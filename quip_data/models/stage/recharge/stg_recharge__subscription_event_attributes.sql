WITH source AS (
	SELECT * FROM {{ ref('stg_recharge__events') }}
)

SELECT
	{{ dbt_utils.generate_surrogate_key([
		'event_id'
		, 'items.attribute'
		]) }} AS event_attribute_id
	, event_id
	, object_id AS subscription_id

	, event_at
	, items.attribute AS field_name
	, LOWER(items.previous_value) AS previous_value
	, LOWER(items.value) AS current_value
FROM source
, UNNEST(updated_attributes) AS items
WHERE object_type = 'subscription'