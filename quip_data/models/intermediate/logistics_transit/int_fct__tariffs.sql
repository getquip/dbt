WITH

tariffs AS (
	SELECT * FROM {{ ref("stg_wen_parker__tariff_details") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	{{ dbt_utils.dbt_utils.generate_surrogate_key([
		'house_bill_number'
		, 'tariff_number'
		]) }} AS tariff_id
	, house_bill_number
	, tariff_number
	, SUM(tariff_duty) AS total_tariff_duty
	, SUM(fees) AS total_fees
	, SUM(tariff_duty) + SUM(fees) AS total_tariff_cost
FROM tariffs
GROUP BY 1,2

