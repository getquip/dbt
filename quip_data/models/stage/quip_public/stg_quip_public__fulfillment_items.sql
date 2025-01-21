{{ config(
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"sku", 
		"fulfillment_request_id", 
		"fulfillment_shipment_id", 
		"line_item_id"
	]
) }}

WITH source AS (
    SELECT * FROM {{ source('quip_public', 'fulfillment_items') }}
)

, renamed AS (
    SELECT
        id AS line_item_id
        , COALESCE(_fivetran_deleted , FALSE) AS is_source_deleted
        , _fivetran_synced AS source_synced_at
        , created_at
        , updated_at
        , fulfillment_request_id
        , fulfillment_shipment_id
        , metadata
        , quantity
        , shopify_line_item_gid
        , shopify_line_item_id
        , sku
        , unit_ounces / 16 AS unit_lbs
        , unit_price
        , FALSE AS is_added_by_fulfillment_app
    FROM source
)

, supplement_records AS (
    SELECT
        CAST(CONCAT(
            line_item_id
            , REPLACE(JSON_EXTRACT_SCALAR(supplement , '$.sku') , '-' , '')
        ) AS INTEGER) AS line_item_id
        , is_source_deleted
        , source_synced_at
        , created_at
        , updated_at
        , fulfillment_request_id
        , fulfillment_shipment_id
        , CAST(NULL AS STRING) AS metadata
        , CAST(JSON_EXTRACT_SCALAR(supplement , '$.quantity') AS INTEGER) AS quantity
        , CAST(NULL AS STRING) AS shopify_line_item_gid -- need to fill in intermediate layer
        , CAST(NULL AS STRING) AS shopify_line_item_id -- need to fill in intermediate layer
        , JSON_EXTRACT_SCALAR(supplement , '$.sku') AS sku
        , NULL AS unit_lbs -- need to fill in intermediate layer
        , NULL AS unit_price -- need to fill in intermediate layer
        , TRUE AS is_added_by_fulfillment_app
    FROM renamed
    , UNNEST(JSON_EXTRACT_ARRAY(metadata , '$.supplements')) AS supplement
    WHERE NOT is_source_deleted
)

SELECT * FROM renamed
WHERE NOT is_source_deleted

UNION ALL

SELECT * FROM supplement_records
