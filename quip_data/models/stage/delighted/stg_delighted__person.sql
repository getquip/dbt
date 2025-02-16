WITH

nps AS (
  SELECT * FROM {{ source("delighted_nps", "person") }}
)

, incentive AS (
  SELECT * FROM {{ source("delighted_incentive", "person") }}
)

, self_service AS (
  SELECT * FROM {{ source("delighted_self_service", "person") }}
)

, main AS (
  SELECT * FROM {{ source("delighted_quip_main", "person") }}
)

SELECT 
  id AS person_id
  , _fivetran_deleted AS is_source_deleted
  , _fivetran_synced AS source_synced_at
  , name
  , TIMESTAMP_SECONDS(created_at) AS created_at
  , email
  , 'quip NPS' AS project_name
FROM nps

UNION ALL

SELECT
  id AS person_id
  , _fivetran_deleted AS is_source_deleted
  , _fivetran_synced AS source_synced_at
  , name
  , TIMESTAMP_SECONDS(created_at) AS created_at
  , email
  , 'quip Incentive' AS project_name
FROM incentive

UNION ALL

SELECT  
  id AS person_id
  , FALSE AS is_source_deleted
  , _fivetran_synced AS source_synced_at
  , NULL AS name
  , created_at
  , email
  , 'LEGACY: quip Self Service' AS project_name
FROM self_service

UNION ALL

SELECT 
  id AS person_id
  , FALSE AS is_source_deleted
  , _fivetran_synced AS source_synced_at
  , NULL AS name
  , created_at
  , email
  , 'LEGACY: quip Main' AS project_name
FROM main