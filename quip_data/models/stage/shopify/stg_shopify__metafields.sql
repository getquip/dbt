
WITH source AS (
    SELECT * FROM {{ source('shopify', 'metafield') }}
)

SELECT
    id AS metafield_id
    , owner_id AS resource_id
    , LOWER(owner_resource) AS resource_table_name
    , `namespace`
    , `key`
    , `value`
    , `type`
    , `description`
    , created_at
    , updated_at
FROM source
