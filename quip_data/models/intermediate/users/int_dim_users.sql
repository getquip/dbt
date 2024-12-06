WITH

shopify_customers AS (
    SELECT * FROM {{ ref('stg_shopify__customers') }}
),