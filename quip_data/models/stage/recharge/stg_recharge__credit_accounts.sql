{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"credit_account_id",
        "recharge_customer_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('recharge', 'credit_accounts') }}
)

SELECT
    id AS credit_account_id
    , customer_id AS recharge_customer_id
    , available_balance
    , created_at
    , currency_code
    , expires_at
    , SAFE_CAST(initial_value AS FLOAT64) AS initial_amount
    , type AS credit_type
    , updated_at
    , source_synced_at
    , LOWER(name) AS credit_name
FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC
    ) = 1
