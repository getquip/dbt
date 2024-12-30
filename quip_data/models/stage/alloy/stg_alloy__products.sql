WITH source AS (
	SELECT * FROM {{ source("quip", "products") }}
)

SELECT
	*
FROM source