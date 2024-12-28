WITH source AS (
	SELECT * FROM {{ source('recharge', 'charges') }}
)

SELECT 
	id AS charge_id
	, address_id
	/* -- can be parsed if we need it
	, analytics_data
	, external_transaction_id
	, billing_address
	, client_details
	, customer
	*/
	, created_at
	, currency
	, -external_variant_not_found AS has_external_variant
	, error
	, charge_attempts AS count_charge_attempts
	, has_uncommitted_changes
	, error_type
	, note
	, merged_at
	, original_scheduled_at
	, payment_processor
	, processed_at
	, retry_date AS retry_at
	, scheduled_at
	, orders_count AS count_orders
	, external_order_id
	, `status`
	, shipping_address
	, tags
	, subtotal_price
	, total_price
	, source_synced_at
	, taxable AS is_taxable
	, taxes_included AS has_taxes_included
	, total_discounts
	, total_line_items_price
	, total_tax
	, total_weight_grams
	, updated_at
	, total_refunds
	, total_duties
	, type
	, last_charge_attempt AS last_charge_attempt_at
FROM source
-- dedupe
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1