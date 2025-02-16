WITH

, incentive AS (
  SELECT * FROM {{ source("delighted_incentive", "response") }}
)

, self_service AS (
  SELECT * FROM {{ source("delighted_self_service", "response") }}
)

, main AS (
  SELECT * FROM {{ source("delighted_quip_main", "response") }}
)

SELECT
  id AS response_id
  , person_id
  , _fivetran_synced AS source_synced_at
  , comment
  , score
  , properties_delighted_source AS delighted_source
  , properties_sfdc_case_origin AS case_origin
  , properties_delighted_inbound_message_id AS inbound_message_id
  , properties_sfdc_case_priority AS case_priority
  , properties_sfdc_case_status AS status
  , TIMESTAMP_SECONDS(created_at) AS created_at
  , TIMESTAMP_SECONDS(updated_at) AS updated_at
  , 'quip Incentive' AS project_name
FROM incentive
WHERE survey_type = 'csat'

UNION ALL

SELECT  
  id AS response_id
  , person_id
  , _fivetran_synced AS source_synced_at
  , comment
  , score
  , properties_delighted_source AS delighted_source
  , NULL AS properties_sfdc_case_origin AS case_origin
  , NULL AS properties_delighted_inbound_message_id AS inbound_message_id
  , NULL AS properties_sfdc_case_priority AS case_priority
  , NULL AS properties_sfdc_case_status AS status
  , created_at
  , updated_at
  , 'LEGACY: quip Self Service' AS project_name
FROM self_service
WHERE survey_type = 'csat'

UNION ALL

SELECT 
  id AS response_id
  , person_id
  , _fivetran_synced AS source_synced_at
  , comment
  , score
  , properties_delighted_source AS delighted_source
  , properties_sfdc_case_origin AS case_origin
  , properties_delighted_inbound_message_id AS inbound_message_id
  , properties_sfdc_case_priority AS case_priority
  , properties_sfdc_case_status AS status
  , created_at
  , updated_at
  , 'LEGACY: quip Main' AS project_name
FROM main