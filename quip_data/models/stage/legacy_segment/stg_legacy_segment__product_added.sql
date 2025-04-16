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
	SELECT * FROM {{ source('legacy_segment', 'quip_production__product_added') }} 
)

SELECT
	id AS event_id
	, `timestamp` AS event_at
	, name AS product_added_name
    , "quip_production" AS source_name
    , user_id
    , anonymous_id
	, `event` AS event_name
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1