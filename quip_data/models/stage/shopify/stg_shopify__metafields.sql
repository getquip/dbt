{{ config(
    partition_by={
      "field": "updated_at",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    },
	cluster_by=[
		"resource_table_name", 
		"type", 
		"key", 
		"metafield_id"]
) }}

WITH source AS (
    SELECT * FROM {{ source('shopify', 'metafield') }}
)

SELECT
    id AS metafield_id
    , owner_id AS resource_id

    , _fivetran_synced AS source_synced_at
    , created_at
    , updated_at

    , `description`
    , `key`
    , `namespace`
    , `type`
    , `value`
    , LOWER(owner_resource) AS resource_table_name
FROM source
