WITH
orders AS (
    SELECT * FROM {{ stg_shopify__orders}}
)

, legacy_orders AS (
    SELECT 'quip_public.orders'
)

SELECT
    -- ids
    order_id
    , shopify_customer_id
    -- timestamps
    , created_at
    , updated_at
    , cancelled_at

    , fulfillment_status
    , payment_status

    -- facts
    , total_weight/ 453.592 AS total_weight_lbs

    -- finance
    , total_discounts_set		
    , total_price_set	
    , total_shipping_price_set	
    , total_tax_set
    , current_subtotal_price	
    , current_total_discounts	
    , current_total_price	
    , current_total_tax	
    , subtotal_price	
    , total_discounts	
    , order_product_total_price
    , order_total_price
    , current_tax_paid	
    , total_tip_received	


FROM orders

-- order_type
-- fulfillment_status
-- payment_status
-- is_only_accessories_or_refills