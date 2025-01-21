WITH source AS (
    SELECT * FROM {{ source('recharge', 'credit_accounts') }}
)

SELECT
    id AS credit_account_id
    , customer_id
    , available_balance
    , created_at
    , currency_code
    , expires_at
    , initial_value
    , type AS credit_applied_by
    , updated_at
    , source_synced_at
    , LOWER(name) AS credit_name
FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC
    ) = 1
