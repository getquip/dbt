WITH source AS (
	SELECT * FROM {{ source('recharge', 'discounts') }}
)

SELECT * FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1