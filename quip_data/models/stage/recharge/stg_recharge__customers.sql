

WITH source AS (
    SELECT * FROM {{ source('recharge', 'customers') }}
)

SELECT * FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1