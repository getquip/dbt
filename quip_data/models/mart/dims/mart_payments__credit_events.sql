
WITH first_credit AS (
	-- identify first credit associated with a credit_account_id
	SELECT
		credit_event_id
		, credit_account_id
		, created_at
	FROM {{ ref('int_fct_payments__credit_events') }}
	WHERE payment_transaction_type = 'credit'
	QUALIFY ROW_NUMBER () OVER (PARTITION BY credit_account_id ORDER BY created_at) = 1
)

, first_debit AS (
	-- identify first debit associated with a credit_account_id
	SELECT
		credit_event_id
		, credit_account_id
		, created_at
	FROM {{ ref('int_fct_payments__credit_events') }}
	WHERE payment_transaction_type = 'debit'
	QUALIFY ROW_NUMBER () OVER (PARTITION BY credit_account_id ORDER BY created_at) = 1
)

SELECT
    e.*
    , CASE 
		WHEN payment_transaction_type = 'debit' 
			THEN TIMESTAMP_DIFF(d.created_at, c.created_at, DAY) 
		END AS days_between_credit_and_debit
FROM {{ ref('int_fct_payments__credit_events') }} e
LEFT JOIN first_credit c
    ON e.credit_account_id = c.credit_account_id
LEFT JOIN first_debit d
	ON e.credit_account_id = d.credit_account_id
