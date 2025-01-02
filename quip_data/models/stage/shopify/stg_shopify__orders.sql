{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "timestamp",
      "granularity": "day"
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

, renamed AS (
    SELECT
        -- ids
        id AS shopify_order_id
        , order_number	
        , user_id AS agent_id
        , checkout_id	
        , company_id	
        , company_location_id	
        , app_id	
        , SAFE_CAST(customer_id AS INTEGER) AS shopify_customer_id
        , location_id	

        -- timestamps
        , _fivetran_synced AS source_synced_at
        , COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
		-- convert to UTC timestamp
	    , created_at
	    , updated_at
        , closed_at	
        , processed_at

        -- strings 
        , client_details_user_agent	
        , currency	
        , order_status_url	
        , payment_gateway_names	
        , presentment_currency	
        , reference	
        , source_identifier	
        , source_name	
        , source_url	
        , customer_locale	
        , email	
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
        , buyer_accepts_marketing	
        , confirmed	AS is_confirmed_inventory
        , taxes_included

        -- cancellations
        , cancelled_at	
        , cancelled_at IS NOT NULL AS is_cancelled
        , cancel_reason	

        -- payments
        , current_subtotal_price	
        , current_total_discounts	
        , current_total_price 
        , current_total_tax	AS tax_at_checkout
        , subtotal_price_set	
        , current_subtotal_price_set	
        , current_total_discounts_set	
        , current_total_duties_set	
        , current_total_price_set	
        , current_total_tax_set	
        , subtotal_price AS subtotal_price_at_checkout
        , total_line_items_price AS product_total_price	
        , total_price AS total_price_at_checkout
        , total_tax	AS current_tax_paid -- excludes refunded taxes
        , total_tip_received	
        , total_discounts_set	
        , total_line_items_price_set	
        , total_price_set	
        , total_shipping_price_set	
        , total_tax_set 
        , original_total_duties_set	
        , financial_status	AS payment_status

        -- ints
        -- docs say that this value is not updated if items are removed from the order
        , total_weight AS total_weight_grams

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
    WHERE NOT test

)

SELECT 
    * 

    -- order attributes
    , IF(payment_status = 'paid' AND fulfillment_status = 'fulfilled', TRUE, FALSE) AS is_completed_order
    , total_weight_grams/ 453.592 AS total_weight_lbs	
FROM renamed
WHERE NOT is_source_deleted