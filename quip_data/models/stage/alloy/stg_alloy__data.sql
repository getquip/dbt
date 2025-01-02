WITH source AS (
	SELECT * FROM {{ source("quip", "data") }}
)

SELECT
FROM source