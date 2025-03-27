SELECT
    event_facts.credit_event_id
	, event_facts.credit_account_id
	, COALESCE(users.shopify_customer_id, users.legacy_customer_id) as user_id
	, event_facts.recharge_charge_id
	, event_facts.payment_transaction_type
	, event_facts.shopify_order_id
	, event_facts.amount
	, event_facts.created_at
	, event_facts.credit_type
    , event_dims.days_between_credit_and_debit
FROM {{ ref('int_fct_payments__credit_events') }} AS event_facts
LEFT JOIN {{ ref('credit_account_dims') }} AS event_dims
    ON event_facts.credit_account_id = event_dims.credit_account_id
LEFT JOIN {{ ref('int_dim_users') }} AS users
    ON event_facts.recharge_customer_id = users.recharge_customer_id