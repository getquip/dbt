WITH source AS (
	SELECT * FROM {{ source('wen_parker', 'tariff_details') }}
)

SELECT DISTINCT
  house_bill_number
  , TRIM(LOWER(line_item_description)) AS line_item_description
  , CAST(REPLACE(REPLACE(tariff_duty, ',',''), '$','') AS FLOAT64) AS tariff_duty
  , tariff_number
  ,  CAST(REPLACE(REPLACE(fees, ',',''), '$','') AS FLOAT64) AS fees
  , CAST(REPLACE(duty_rate_percent, '%','') AS FLOAT64) / 100 AS duty_rate_percent
  , CAST(duty_rate AS FLOAT64) AS duty_rate
FROM source