WITH source_table AS (
	SELECT * FROM {{ source('recharge', 'events') }}
)

, renamed AS (
	SELECT
		id AS event_id
		, customer_id AS recharge_customer_id
		, object_id
		, created_at AS event_at
		, description
		, object_type
		, `source`.account_id AS admin_account_id
		, `source`.api_token_id
		, `source`.account_email AS admin_account_email
		, `source`.api_token_name
		, `source`.origin AS event_origin
		, `source`.user_type
		, verb

		-- nested fields
		, `custom_attributes`
		, `updated_attributes`
	FROM source_table
)

SELECT * FROM renamed
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY event_at DESC) = 1