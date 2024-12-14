WITH source AS (
    SELECT * FROM {{ source('shopify', 'order') }}
)

, renamed AS (
    SELECT
        -- ids
        id AS order_id
        , order_number	
        , user_id AS agent_id
        , company_id	
        , company_location_id	
        , app_id	
        , customer_id AS user_id
        , location_id	

        -- timestamps
        , _fivetran_synced AS source_synced_at
        , _fivetran_deleted AS is_source_deleted
        , created_at	
        , updated_at	
        , cancelled_at	
        , closed_at	
        , processed_at	

        -- strings 
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
        , browser_ip	
        , cancel_reason	
        , cart_token	
        , checkout_id	
        , checkout_token	
        , client_details_user_agent	
        , currency	
        , order_status_url	
        , original_total_duties_set	
        , payment_gateway_names	
        , presentment_currency	
        , reference	
        , referring_site	
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
        , source_identifier	
        , source_name	
        , source_url	
        , subtotal_price_set	
        , current_subtotal_price_set	
        , current_total_discounts_set	
        , current_total_duties_set	
        , current_total_price_set	
        , current_total_tax_set	
        , customer_locale	
        , device_id	
        , email	
        , financial_status	AS payment_status
        , fulfillment_status	
        , landing_site_base_url	
        , landing_site_ref	
        , name	
        , note	
        , note_attributes	
        , token	
        , total_discounts_set	
        , total_line_items_price_set	
        , total_price_set	
        , total_shipping_price_set	
        , total_tax_set

        -- bools
        , buyer_accepts_marketing	
        , confirmed	
        , taxes_included	
        , test	

        -- floats
        , current_subtotal_price	
        , current_total_discounts	
        , current_total_price	
        , current_total_tax	
        , subtotal_price	
        , total_discounts	
        , total_line_items_price	
        , total_price	
        , total_tax	
        , total_tip_received	

        -- ints
        , total_weight AS total_weight_grams
        , total_weight/ 453.592 AS total_weight_lbs
        , number	
    FROM source

)

SELECT * FROM renamed
WHERE NOT is_source_deleted