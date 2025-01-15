WITH source AS (
	SELECT * FROM {{ source('wen_parker', 'tariff_details') }}
)

, cleaned AS (
  SELECT DISTINCT
    house_bill_number
    , TRIM(LOWER(line_item_description)) AS line_item_description
    , CAST(REPLACE(REPLACE(tariff_duty, ',',''), '$','') AS FLOAT64) AS tariff_duty
    , tariff_number
    , CAST(REPLACE(REPLACE(fees, ',',''), '$','') AS FLOAT64) AS fees
    , CAST(REPLACE(duty_rate_percent, '%','') AS FLOAT64) / 100 AS duty_rate_percent
    , CAST(duty_rate AS FLOAT64) AS duty_rate
  FROM source
)
/*
	Tariffs from Wen Parker contain multiple rows per tariff_number because each tariff 
	summary contains one line item per item type. We can aggregate charges to the 
	tariff_number level by house_bill_number
*/
SELECT
	{{ dbt_utils.generate_surrogate_key([
		'house_bill_number'
		, 'tariff_number'
		]) }} AS tariff_id
	, house_bill_number
	, tariff_number
	, SUM(tariff_duty) AS total_tariff_duty
	, SUM(fees) AS total_fees
	, SUM(tariff_duty) + SUM(fees) AS total_tariff_cost
FROM cleaned
GROUP BY 1,2,3


