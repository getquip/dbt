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

WITH RECURSIVE

events AS (
	SELECT * FROM {{ ref("base_customer_data_platform__legacy_events") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, event_sequence AS (
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY anonymous_id ORDER BY event_at) AS event_sequence
	FROM events
)

, new_session_flags AS (
    SELECT
        curr.event_id
        , curr.event_sequence
        -- Check for new session conditions
        , CASE
            -- new session if the campaign utms change
            WHEN (curr.campaign IS NOT NULL AND prev.campaign IS NOT NULL)
                AND (curr.campaign NOT LIKE '%password_reset%' OR prev.campaign NOT LIKE '%password_reset%')
                AND curr.campaign != prev.campaign
                THEN 1

            -- new session if the context_device_type changes (does not capture if from or to NULL)
            WHEN (curr.context_device_type IS NOT NULL AND prev.context_device_type IS NOT NULL)
                AND curr.context_device_type != prev.context_device_type
                THEN 1

            -- new session if os name changes (does not capture if from or to NULL)
            WHEN (curr.context_os_name IS NOT NULL AND prev.context_os_name IS NOT NULL)
                AND curr.context_os_name != prev.context_os_name 
                THEN 1

            -- new session if 30 mins since last event or passes midnight
            WHEN TIMESTAMP_DIFF(curr.event_at, prev.event_at, MINUTE) > 30 
                OR DATE(curr.event_at) != DATE(prev.event_at) 
                THEN 1
                
            ELSE 0
        END AS session_change_flag
    FROM event_sequence AS curr
    LEFT JOIN event_sequence AS prev
        ON curr.anonymous_id = prev.anonymous_id
        AND curr.event_sequence = prev.event_sequence + 1  -- Use event_sequence to join to the previous event
)

, create_sessions AS (
    SELECT
        events.*
        , new_session_flags.event_sequence
        -- Generate session_id based on anonymous_id and session_change_flag
        , CASE 
            WHEN new_session_flags.session_change_flag = 1 
                THEN {{ dbt_utils.generate_surrogate_key(['events.anonymous_id', 'new_session_flags.event_id']) }}
            WHEN new_session_flags.event_sequence = 1
                THEN {{ dbt_utils.generate_surrogate_key(['events.anonymous_id']) }}
        END AS session_id
    FROM events
    INNER JOIN new_session_flags
        ON events.event_id = new_session_flags.event_id
)

, event_sessions AS (
    SELECT
        * EXCEPT(session_id)
        , session_id
    FROM create_sessions
    WHERE event_sequence = 1
        OR session_id IS NOT NULL

    UNION ALL

    SELECT
        curr.* EXCEPT(session_id)
        , COALESCE(curr.session_id, prev.session_id) AS session_id
    FROM create_sessions AS curr
    INNER JOIN event_sessions AS prev
        ON curr.session_id IS NULL
        AND curr.anonymous_id = prev.anonymous_id
        AND curr.event_sequence = prev.event_sequence + 1
)

SELECT * FROM event_sessions