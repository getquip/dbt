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
        "identifies_event_id"
    ]
) }}

WITH

rudderstack AS (
	SELECT * FROM {{ ref('stg_rudderstack__identifies') }}
)

, littledata AS (
    SELECT * FROM {{ ref('stg_littledata__identifies') }}
)

, legacy AS (
    SELECT * FROM {{ ref('stg_legacy_segment__identifies') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, known_users AS (
	SELECT DISTINCT
		user_id
		, anonymous_id
        , source_name
	FROM rudderstack
	WHERE user_id IS NOT NULL

    UNION ALL

	SELECT DISTINCT
		user_id
		, anonymous_id
        , source_name
	FROM littledata
	WHERE user_id IS NOT NULL

    UNION ALL
    
	SELECT DISTINCT
		user_id
		, anonymous_id
        , source_name
	FROM legacy
	WHERE user_id IS NOT NULL
)

SELECT
    anonymous_id
    , user_id
    , source_name
FROM known_users