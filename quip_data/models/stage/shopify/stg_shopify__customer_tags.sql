WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer_tag') }}
)

SELECT
    -- ids
    customer_id AS shopify_user_id

    -- timestamps
    , _fivetran_synced AS source_synced_at

    -- strings
    , LOWER(value) AS tag
FROM source

