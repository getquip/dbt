{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    },
	cluster_by=[
		"payment_status", 
		"fulfillment_status", 
		"shopify_customer_id", 
		"shopify_order_id"]
)}}

WITH source AS (
    SELECT * FROM {{ source('shopify', 'order') }}
)
/*
NOTE: some fields are purposely excluded from this model. They are either:
- redundant (e.g. total_price_set is a nested field that contains the same information as total_price)
- possibly stale
- deprecated (e.g. total_discounts)

Confirm with the API docs before adding any of these fields back in.
https://shopify.dev/docs/api/admin-rest/2024-10/resources/order#resource-object
*/

, renamed AS (
    SELECT
        -- ids
        id AS shopify_order_id
        , user_id AS agent_id
        , checkout_id	
        , company_id	
        , company_location_id	
        , app_id	
        , SAFE_CAST(customer_id AS INTEGER) AS shopify_customer_id

        , _fivetran_synced AS source_synced_at
        , COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
	    , created_at
	    , updated_at
        , processed_at

        -- strings 
        , client_details_user_agent
        , presentment_currency	
        , reference	
        , source_identifier	
        , LOWER(source_name) AS source_name
        , source_url	
        , customer_locale	
        , fulfillment_status	
        , name	
        , note	
        , note_attributes
        
        -- utm etc
        , landing_site_base_url	
        , landing_site_ref	
        , referring_site	
        , browser_ip	
        , device_id	

        -- bools
        , confirmed	AS is_confirmed_inventory
        , test AS is_test

        -- cancellations
        , cancelled_at
        , cancel_reason	

        -- payments
        , payment_gateway_names	
        , currency
        , financial_status	AS payment_status
        , total_tip_received	
        ---- metrics at checkout
        , current_total_tax	AS total_tax_at_checkout -- naming convention is confusing, but confirmed in the docs
        , IF(taxes_included, subtotal_price - current_total_tax, subtotal_price) AS subtotal_price_at_checkout
        , total_line_items_price AS total_product_price_at_checkout
        , total_price AS total_price_at_checkout
        , total_weight AS total_weight_grams
        , total_weight * 0.00220462 AS total_weight_lbs
        ---- metrics at current time: this reflects the amount after any edits/refunds
        , current_total_discounts
        , current_total_price
        , current_subtotal_price
        , total_tax	AS current_total_tax

        -- address
        , shipping_address_address_1	
        , shipping_address_address_2	
        , shipping_address_city	
        , shipping_address_company	
        , shipping_address_country	
        , shipping_address_country_code	
        , shipping_address_first_name	
        , shipping_address_last_name	
        , shipping_address_latitude	
        , shipping_address_longitude	
        , shipping_address_name	
        , shipping_address_phone	
        , shipping_address_province	
        , shipping_address_province_code	
        , shipping_address_zip	
        , billing_address_address_1	
        , billing_address_address_2	
        , billing_address_city	
        , billing_address_company	
        , billing_address_country	
        , billing_address_country_code	
        , billing_address_first_name	
        , billing_address_last_name	
        , billing_address_latitude	
        , billing_address_longitude	
        , billing_address_name	
        , billing_address_phone	
        , billing_address_province	
        , billing_address_province_code	
        , billing_address_zip	
    FROM source
)

SELECT 
    * 
    -- order attributes
    , IF(fulfillment_status = 'fulfilled', TRUE, FALSE) AS is_completed_order
FROM renamed
WHERE NOT is_source_deleted