WITH source AS (
	SELECT * FROM {{ source('shopify', 'metafield') }}
)

SELECT 
	id AS metafield_id
	, owner_id AS resource_id

	, _fivetran_synced AS source_synced_at
		-- convert to UTC timestamp
	, PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', CAST(created_at AS STRING)) AS created_at
	, PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', CAST(updated_at AS STRING)) AS updated_at
	 
	, `description`
	, `key`
	, `namespace`
	, LOWER(owner_resource) AS resource_table_name
	, `type`
	, `value`
FROM source