WITH source AS (
	SELECT * FROM {{ source("quip", "locations") }}
)

SELECT
	location_id

	, LOWER(Postal_Area_Name) AS postal_area_name
	, LOWER(City) AS city
	, LOWER(County) AS county
	, UPPER(State) AS state
	, LOWER(Sub_region) AS sub_region

	, Close_Date AS close_date
	, IF(Close_Date IS NOT NULL, TRUE, FALSE) AS is_closed
	, Open_Date AS open_date
	, IF(Open_Date IS NOT NULL, TRUE, FALSE) AS is_open

	, LOWER(Location_Name) AS location_name
	, LOWER(Location_Type) AS location_type
	, LOWER(Location_Tier) AS location_tier
	, LOWER(Fulfillment_Method) AS fulfillment_method
	, LOWER(Inventory_Type) AS inventory_type

	, Address1 AS address
	, Postal_Code AS postal_code
	, LOWER(COALESCE(Region, Census_Region)) AS region
FROM source