WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer') }}
)

SELECT
    _fivetran_synced AS synced_at
    _fivetran_deleted AS is_source_deleted
    , * EXCEPT(_fivetran_synced, _fivetran_deleted)
FROM source
WHERE NOT _fivetran_deleted
