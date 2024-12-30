WITH 

partner_ids AS (
	SELECT DISTINCT partner_id, location_id FROM {{ source("quip", "data") }}
)

, locations AS (
	SELECT DISTINCT location_id, partner FROM {{ ref("stg_alloy__locations") }}
)

SELECT DISTINCT
  partner_ids.partner_id
  , locations.partner AS partner_name
FROM partner_ids
INNER JOIN locations
  ON partner_ids.location_id = locations.location_id