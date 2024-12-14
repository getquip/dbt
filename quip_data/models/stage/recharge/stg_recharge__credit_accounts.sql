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
	, LOWER(name) AS credit_name
	, type AS credit_applied_by
	, updated_at 
FROM source