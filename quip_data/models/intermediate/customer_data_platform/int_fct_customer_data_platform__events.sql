
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "session_id", 
        "anonymous_id",
        "event_id"
    ]
) }}

WITH

events AS (
    SELECT * FROM {{ ref("int_fct_customer_data_platform__event_sessions") }}
    WHERE event_at >= {{ get_max_partition('event_at', lookback_window=30) }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, page_event_sequence AS (
    SELECT
        event_id
        , session_id
        , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_at, event_id) AS page_event_sequence
        , TIMESTAMP_DIFF(LEAD(event_at) OVER 
            (PARTITION BY session_id, context_page_path_scrubbed ORDER BY event_at), event_at, SECOND) AS page_time_spent_seconds
    FROM events
    WHERE event_type IN ('page', 'screen')
)

, track_event_sequence AS (
    SELECT
        event_id
        , session_id
        , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_at, event_id) AS track_event_sequence
    FROM events
    WHERE event_type = 'track'
)

SELECT
    *
    , pages.page_event_sequence
    , pages.page_time_spent_seconds
    , tracks.track_event_sequence
    , {{ scrub_context_page_path('context_page_path') }}
	, {{ parse_server_side_event('context_library_name') }}
    , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_at, event_id) AS event_sequence
FROM events
LEFT JOIN page_event_sequence AS pages
    ON event_id = pages.event_id
LEFT JOIN track_event_sequence AS tracks
    ON event_id = tracks.event_id
