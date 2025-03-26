{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "TIMESTAMP",
      "granularity": "DAY"
    },
	cluster_by=[
        "tag", 
        "shopify_customer_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('shopify', 'customer_tag') }}
)

SELECT
    -- ids
    customer_id AS shopify_customer_id

    -- timestamps
    , _fivetran_synced AS source_synced_at

    -- strings
    , LOWER(value) AS tag
FROM source
