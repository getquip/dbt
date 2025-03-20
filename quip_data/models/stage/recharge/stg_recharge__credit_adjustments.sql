WITH source AS (
    SELECT * FROM {{ source('recharge', 'credit_adjustments') }}
)

SELECT
    id AS credit_adjustment_id
    , credit_account_id
    , amount AS adjustment_amount
    , currency_code
    , created_at
    , ending_balance
    , note
    , type AS adjustment_type
    , updated_at
    , created_by.resource_id AS created_by_resource_id
    , created_by.type AS created_by_type
    , LOWER(created_by.identifier) AS created_by_identifier
FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY credit_adjustment_id
        ORDER BY updated_at DESC
    ) = 1
