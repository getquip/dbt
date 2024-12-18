SELECT 
  date
  , EXTRACT(YEAR FROM date) AS year
  , EXTRACT(MONTH FROM date) AS month
  , EXTRACT(DAY FROM date) AS day
  , EXTRACT(DAYOFWEEK FROM date) AS day_of_week
  , EXTRACT(DAYOFYEAR FROM date) AS day_of_year
FROM 
  UNNEST(
	GENERATE_DATE_ARRAY(
		'2013-12-31'
		, CURRENT_DATE()
		, INTERVAL 1 DAY)
	) AS date
