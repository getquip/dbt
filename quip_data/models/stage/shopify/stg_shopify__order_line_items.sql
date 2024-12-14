WITH source AS (
    SELECT * FROM {{ source('shopify',  'order_line') }}
)

, renamed AS (
    SELECT
        -- ids
        id	
        ,  order_id	

        -- timestamps

        ,  _fivetran_synced AS source_synced_at

        -- strings
        , variant_inventory_management	
        , variant_title	
        , vendor	
        , fulfillment_status	
        , LOWER(name) AS product_name	
        , pre_tax_price_set	
        , price_set	
        , properties	
        , sku	
        , tax_code	
        , title	
        , total_discount_set	
        
        -- ints
        , fulfillable_quantity	
        , grams	
        , index	
        , product_id	
        , quantity	
        , variant_id	

        -- floats
        , pre_tax_price	
        , price	
        , total_discount	
        
        -- bools
        , gift_card	
        , product_exists	
        , requires_shipping	
        , taxable	
)