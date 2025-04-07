-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-refresh
{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "event_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source('legacy_segment', 'quip_production__click') }} 
)

SELECT
	id AS event_id
	, `timestamp` AS event_at
	, target_location
	, target_text
	, target_type
    , target_name
    , "quip_production" AS source_name
    , user_id
    , anonymous_id
	, `event` AS event_name
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1