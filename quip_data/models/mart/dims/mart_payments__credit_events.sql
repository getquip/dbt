
WITH first_credit_debit AS (
	SELECT
	credit_account_id
	, MIN(IF(payment_transaction_type = 'credit', created_at, NULL)) AS first_credit
	, MIN(IF(payment_transaction_type = 'debit', created_at, NULL)) AS first_debit
	FROM {{ ref('int_fct_payments__credit_events') }}
	GROUP BY 1
)

SELECT
    e.*
    , TIMESTAMP_DIFF(f.first_debit, f.first_credit, DAY) AS days_between_credit_and_debit
FROM {{ ref('int_fct_payments__credit_events') }} e
LEFT JOIN first_credit_debit f
    ON e.credit_account_id = f.credit_account_id
