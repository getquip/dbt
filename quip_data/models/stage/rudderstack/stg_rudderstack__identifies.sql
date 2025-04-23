{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
        "field": "first_identified_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "session_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'identifies') }}
	WHERE received_at >= '2025-04-01'
	{% if is_incremental() %}
		AND received_at >= "{{ get_max_partition('first_identified_at', lookback_window = 10) }}"
	{% endif %}
)

, tracks AS (
	SELECT * FROM {{ source('rudderstack_prod', 'tracks') }}
	WHERE received_at >= '2025-04-01'
	{% if is_incremental() %}
		AND received_at >= "{{ get_max_partition('first_identified_at', lookback_window = 10) }}"
	{% endif %}
)

, pages AS (
	SELECT * FROM {{ source('rudderstack_prod', 'pages') }}
	WHERE received_at >= '2025-04-01'
	{% if is_incremental() %}
		AND received_at >= "{{ get_max_partition('first_identified_at', lookback_window = 10) }}"
	{% endif %}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, known_users AS (
	SELECT
		anonymous_id
		, user_id
		, context_session_id
		, MIN(`timestamp`) AS event_at
	FROM source
	WHERE user_id IS NOT NULL
		AND context_session_id IS NOT NULL
	GROUP BY 1, 2, 3

	UNION DISTINCT

	SELECT 
		anonymous_id
		, user_id
		, context_session_id
		, MIN(`timestamp`) AS event_at
	FROM pages
	WHERE user_id IS NOT NULL
		AND context_session_id IS NOT NULL
	GROUP BY 1, 2, 3

	UNION DISTINCT

	SELECT 
		anonymous_id
		, user_id
		, context_session_id
		, MIN(`timestamp`) AS event_at
	FROM tracks
	WHERE user_id IS NOT NULL
		AND context_session_id IS NOT NULL
	GROUP BY 1, 2, 3
)


SELECT
	"rudderstack" AS source_name
	, user_id
	, anonymous_id
	, CAST(context_session_id AS STRING) AS session_id
	, MIN(event_at) AS first_identified_at
FROM known_users
GROUP BY 1, 2, 3, 4