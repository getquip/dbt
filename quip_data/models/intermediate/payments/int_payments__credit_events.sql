{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"payment_transaction_type",
        "recharge_customer_id",
		"shopify_order_id",
		"credit_event_id"
    ]
) }}

WITH

credit_accounts AS (
	SELECT * FROM {{ ref("stg_recharge__credit_accounts") }}
)

, credit_adjustments AS (
	SELECT * FROM {{ ref("stg_recharge__credit_adjustments") }}
)

, charges AS (
	SELECT * FROM {{ ref("stg_recharge__charges") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, transactions AS (
	-- parse credit transactions from charges
	SELECT
		charges.recharge_charge_id
		, charges.recharge_customer_id
		, charges.shopify_order_id
		, transactions.external_transaction_id AS credit_adjustment_id
		, SAFE_CAST(transactions.amount AS FLOAT64) AS amount
		, transactions.created_at AS created_at
		, IF(transactions.kind = 'sale', 'debit', 'credit') AS payment_transaction_type
		, IF(transactions.kind = 'refund', 'refund', 'unapplicable') AS credit_type
	FROM charges
	, UNNEST(transactions) AS transactions
	WHERE transactions.processor_name = 'recharge_credits'
		AND charges.status IN ('partially_refunded', 'success', 'refunded')
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY transactions.payment_method_id
		ORDER BY transactions.created_at DESC
	) = 1

)

, credit_events AS (
	-- get credit issued event
	SELECT
		{{ dbt_utils.generate_surrogate_key(
			[
				'credit_account_id'
				, 'recharge_customer_id'
				, 'credit_type'
				, 'created_at'
			]
		) }} AS credit_event_id
		, credit_account_id
		, recharge_customer_id
		, NULL AS recharge_charge_id
		, 'credit' AS payment_transaction_type
		, NULL AS shopify_order_id
		, initial_amount AS amount
		, created_at
		, credit_type
	FROM credit_accounts

	UNION ALL


	-- join transactions to adjustments for account level info
	SELECT
		{{ dbt_utils.generate_surrogate_key(
			[
				'transactions.credit_adjustment_id'
			]
		) }} AS credit_event_id
		, adjustments.credit_account_id
		, transactions.recharge_customer_id
		, transactions.recharge_charge_id
		, COALESCE(transactions.payment_transaction_type, adjustments.adjustment_type) AS payment_transaction_type
		, transactions.shopify_order_id
		, transactions.amount
		, COALESCE(transactions.created_at, adjustments.created_at) AS created_at
		, transactions.credit_type
	FROM transactions
	LEFT JOIN credit_adjustments AS adjustments
		ON adjustments.credit_adjustment_id = transactions.credit_adjustment_id
)

, first_credit AS (
	-- identify first credit associated with a credit_account_id
	SELECT
		credit_event_id
		, credit_account_id
		, created_at
	FROM credit_events
	WHERE payment_transaction_type = 'credit'
	QUALIFY ROW_NUMBER () OVER (PARTITION BY credit_account_id ORDER BY created_at) = 1
)

, first_debit AS (
	-- identify first debit associated with a credit_account_id
	SELECT
		credit_event_id
		, credit_account_id
		, created_at
	FROM credit_events
	WHERE payment_transaction_type = 'debit'
	QUALIFY ROW_NUMBER () OVER (PARTITION BY credit_account_id ORDER BY created_at) = 1
)

SELECT
    e.*
    , CASE 
		WHEN payment_transaction_type = 'debit' 
			THEN TIMESTAMP_DIFF(d.created_at, c.created_at, DAY) 
		END AS days_between_credit_and_debit
FROM credit_events e
LEFT JOIN first_credit c
    ON e.credit_account_id = c.credit_account_id
LEFT JOIN first_debit d
	ON e.credit_account_id = d.credit_account_id
