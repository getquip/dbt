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
	SELECT * FROM {{ source("legacy_segment", "quip_production__order_completed") }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		"quip_production" AS source_name
		, COALESCE(id, event_id) AS event_id
		, order_id AS checkout_id
		, anonymous_id
		, context_ip
		, context_library_name
		, context_library_version
		, `timestamp` AS event_at
		, loaded_at AS updated_at
		, user_id
		, uuid_ts
		, received_at
	FROM source
)


SELECT 
	* 
	, context_library_name != 'analytics.js' AS is_server_side
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC ) = 1
