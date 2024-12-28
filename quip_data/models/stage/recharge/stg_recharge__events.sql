WITH source AS (
	SELECT * FROM {{ source('recharge', 'events') }}
)

SELECT * FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC) = 1