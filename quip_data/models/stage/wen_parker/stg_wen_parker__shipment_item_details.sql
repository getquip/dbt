{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "sku",
        "po_number", 
		"house_bill_number",
        "shipment_item_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('wen_parker', 'shipment_item_details') }}
)

/*
  We need to aggregate this data to the po_number because occassionally there are 2 line items per sku.
  According to Wen Parker, the data exports we receive are missing a "priority" field that should be
  used to dedupe.

  Example: We have two rows for the sku, but each row's quantity is correct, and when aggregated,
  equals the actual PO/shipment level quantity for the sku.
    - house_bill_number = XMNA00466444
    - po_number = PO0001822
    - sku_number = 900-00110
*/

, renamed AS (
    SELECT
    {{
      dbt_utils.generate_surrogate_key([
        'house_bill_number'
        , 'po_number'
        , 'sku_number'
      ])
    }} AS shipment_item_id
        , house_bill_number
        , po_number
        , sku_number AS sku -- remove non-numeric characters
        , CAST(REPLACE(cartons , ',' , '') AS INTEGER) AS cartons
        , CAST(REPLACE(quantity , ',' , '') AS INTEGER) AS quantity
        , source_synced_at
        , source_file_name
    FROM source
)

, dedupe_by_file AS (
    SELECT
        shipment_item_id
        , house_bill_number
        , po_number
        , sku
        , source_file_name
        , SUM(cartons) AS cartons
        , SUM(quantity) AS quantity
        , MAX(source_synced_at) AS source_synced_at
    FROM renamed
    GROUP BY 1 , 2 , 3 , 4 , 5
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                shipment_item_id
                , house_bill_number
                , source_file_name
                , po_number
                , sku
            ORDER BY source_synced_at DESC
        ) = 1
)

-- dedupe by shipment_item_id
SELECT
    shipment_item_id
    , house_bill_number
    , po_number
    , sku
    , cartons
    , quantity
FROM dedupe_by_file
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY shipment_item_id
        ORDER BY source_synced_at DESC
    ) = 1
