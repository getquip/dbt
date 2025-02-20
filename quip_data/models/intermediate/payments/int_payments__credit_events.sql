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
		recharge_charge_id
		, recharge_customer_id
		, shopify_order_id
		, transactions.external_transaction_id AS credit_adjustment_id
		, SAFE_CAST(transactions.amount AS FLOAT64) AS amount
		, transactions.created_at AS created_at
		, IF(transactions.kind != 'refund', 'debit', 'credit') AS payment_transaction_type
		, IF(transactions.kind = 'refund', 'refund', 'unapplicable') AS credit_type
	FROM charges
	INNER JOIN UNNEST(transactions) AS transactions
		ON transactions.processor_name = 'recharge_credits'
	WHERE charges.status IN ('partially_refunded', 'success', 'refunded')

)

, unioned AS (
	-- get credit issued event
	SELECT
		credit_account_id
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
		adjustments.credit_account_id
		, transactions.recharge_customer_id
		, transactions.recharge_charge_id
		, COALESCE(transactions.payment_transaction_type, adjustments.adjustment_type) AS payment_transaction_type
		, transactions.shopify_order_id
		, transactions.amount
		, COALESCE(transactions.created_at, adjustments.created_at) AS created_at
		, transactions.credit_type
	FROM transactions
	LEFT JOIN credit_adjustments AS adjustments
		ON adjustments.credit_adjustment_id = adjustments.credit_adjustment_id
)

SELECT
	*
	, {{ dbt_utils.generate_surrogate_key(
		[
			'credit_account_id'
			, 'recharge_customer_id'
			, 'recharge_charge_id'
			, 'payment_transaction_type'
			, 'credit_type'
			, 'created_at'
		]
	) }} AS credit_event_id
FROM unioned


