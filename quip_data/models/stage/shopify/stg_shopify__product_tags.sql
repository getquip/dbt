{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"tag", 
		"product_id"]
)}}

WITH source AS (
	SELECT * FROM {{ source('shopify', 'product_tag') }}
)

SELECT
	index
	, product_id
	, _fivetran_synced AS source_synced_at
	, LOWER(value) AS tag
FROM source