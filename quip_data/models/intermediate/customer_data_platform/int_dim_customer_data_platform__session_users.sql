{{ config(
    materialized='table',
    partition_by={
        "field": "first_identified_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "shopify_customer_id", 
        "anonymous_id",
        "session_id"
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

, dim_sessions AS (
    SELECT * FROM {{ ref('int_dim_customer_data_platform__sessions') }}
)

, users AS (
    SELECT * FROM {{ ref('int_dim_users') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, known_users AS (
	SELECT
        SAFE_CAST(user_id AS INTEGER) AS shopify_customer_id
        , anonymous_id
        , source_name
        , session_id
        , first_identified_at
    FROM rudderstack

    UNION ALL

	SELECT
		SAFE_CAST(user_id AS INTEGER) AS shopify_customer_id
		, anonymous_id
        , source_name
        , NULL AS session_id
        , MIN(event_at) AS first_identified_at
	FROM littledata
	WHERE user_id IS NOT NULL
    GROUP BY 1, 2, 3

    UNION ALL
    
	SELECT
		users.shopify_customer_id
		, legacy.anonymous_id
        , legacy.source_name
        , NULL AS session_id
        , MIN(legacy.event_at) AS first_identified_at
	FROM legacy
    LEFT JOIN users
        ON legacy.user_id = users.legacy_segment_user_id
	WHERE user_id IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    dim_sessions.session_id
    , dim_sessions.anonymous_id
    , dim_sessions.source_name
    , known_users.shopify_customer_id
    , known_users.first_identified_at
FROM dim_sessions
LEFT JOIN known_users
    ON known_users.source_name = dim_sessions.source_name
    AND known_users.session_id = dim_sessions.session_id
    OR (known_users.anonymous_id = dim_sessions.anonymous_id
        AND known_users.first_identified_at BETWEEN dim_sessions.session_start_at AND dim_sessions.session_end_at)
