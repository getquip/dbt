{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "is_pure_experiment_session",
        "anonymous_id", 
        "session_id",
		"event_id"
    ]
) }}

WITH

events AS (
	SELECT * FROM {{ ref('base_customer_data_platform__legacy_sessions') }}
)

, experiments AS (
	SELECT * FROM {{ ref('stg_legacy_segment__experiment_viewed') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, context AS (
	SELECT
		events.anonymous_id
		, events.user_id
		, events.session_id
		, events.event_at
		, experiments.experiment_id
		, experiments.experiment_variant_id
		, COUNT(DISTINCT experiments.experiment_variant_id) OVER
			(PARTITION BY events.session_id, experiments.experiment_id) AS experiment_variant_count
		, COUNT(DISTINCT experiments.experiment_id) OVER
			(PARTITION BY events.session_id)  AS experiment_count
	FROM events
	INNER JOIN experiments
		ON events.event_id = experiments.event_id
		AND events.context_page_scrubbed = 'experiment_viewed'
		AND experiments.experiment_variant_id IS NOT NULL
)

SELECT
	*
	, experiment_variant_count > 1 AS is_variant_contaminated
	, experiment_count > 1 AS is_experiment_contaminated
	, experiment_variant_count = 1 AND experiment_count = 1 AS is_pure_experiment_session
FROM context