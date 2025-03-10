
WITH base AS (

	SELECT
	credit_account_id
	, MIN(IF(payment_transaction_type = 'credit', created_at, NULL)) AS first_credit
	, MIN(IF(payment_transaction_type = 'debit', created_at, NULL)) AS first_debit
	FROM {{ ref('int_fct_payments__credit_events') }}
	GROUP BY 1

)

SELECT 
credit_account_id
, TIMESTAMP_DIFF(first_debit, first_credit, DAY) AS days_between_credit_and_debit
FROM base