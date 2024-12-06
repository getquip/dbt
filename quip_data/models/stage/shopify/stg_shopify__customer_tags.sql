WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer_tag') }}
)

SELECT
    _fivetran_synced AS source_synced_at
    , * EXCEPT(_fivetran_synced)
FROM source
