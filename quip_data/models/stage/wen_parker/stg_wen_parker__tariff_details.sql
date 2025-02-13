{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
		"house_bill_number",
        "tariff_number"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('wen_parker', 'tariff_details') }}
)

, cleaned AS (
    SELECT
        house_bill_number
        , tariff_number
        , source_synced_at
        , source_file_name
        , TRIM(LOWER(line_item_description)) AS line_item_description
        , CAST(REPLACE(REPLACE(tariff_duty , ',' , '') , '$' , '') AS FLOAT64)
            AS tariff_duty
        , CAST(REPLACE(REPLACE(fees , ',' , '') , '$' , '') AS FLOAT64) AS fees
        , CAST(REPLACE(duty_rate_percent , '%' , '') AS FLOAT64)
        / 100 AS duty_rate_percent
        , CAST(duty_rate AS FLOAT64) AS duty_rate
    FROM source

)
/*
	Tariffs from Wen Parker contain multiple rows per tariff_number because each tariff
	summary contains one line item per item type. We can aggregate charges to the
	tariff_number level by house_bill_number
*/

, dedupe_by_file AS (
    SELECT
        house_bill_number
        , tariff_number
        , source_file_name
        , MAX(source_synced_at) AS source_synced_at
        , SUM(tariff_duty) AS total_tariff_duty
        , SUM(fees) AS total_fees
        , SUM(tariff_duty) + SUM(fees) AS total_tariff_cost
    FROM cleaned
    GROUP BY 1 , 2 , 3
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY house_bill_number , tariff_number , source_file_name
            ORDER BY source_synced_at DESC
        ) = 1
)

SELECT
	{{ dbt_utils.generate_surrogate_key([
      'house_bill_number'
      , 'tariff_number'
      ]) }} AS tariff_id
    , house_bill_number
    , tariff_number
    , total_tariff_duty
    , total_fees
    , total_tariff_cost
FROM dedupe_by_file
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY tariff_id
        ORDER BY source_synced_at DESC
    ) = 1
