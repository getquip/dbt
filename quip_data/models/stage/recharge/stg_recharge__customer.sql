WITH source AS (
    SELECT * FROM {{ source('recharge', 'customer') }}
)

SELECT * FROM source