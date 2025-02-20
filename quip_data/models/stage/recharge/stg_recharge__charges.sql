{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "status", 
		"charge_type",
        "recharge_charge_id"
    ]
) }}


WITH source AS (
    SELECT * FROM {{ source('recharge', 'charges') }}
)

, renamed AS (
    SELECT
        id AS recharge_charge_id
        , address_id
        , customer.id AS recharge_customer_id
        , customer.external_customer_id.ecommerce AS shopify_customer_id
        /* -- can be parsed if we need it
        , analytics_data
        , external_transaction_id
        , billing_address
        , client_details
        */
        , created_at
        , currency
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
        , external_order_id.ecommerce AS shopify_order_id
        , `status`
        , shipping_address
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
        , `type` AS charge_type
        , last_charge_attempt AS last_charge_attempt_at
        , IF(external_variant_not_found IS NULL , FALSE , TRUE) AS has_external_variant
        
        , tags
        , include.transactions AS transactions
    FROM source
    -- dedupe
    
)



SELECT * EXCEPT (tags)
FROM renamed
QUALIFY ROW_NUMBER() OVER (
            PARTITION BY recharge_charge_id
            ORDER BY updated_at DESC
        ) = 1