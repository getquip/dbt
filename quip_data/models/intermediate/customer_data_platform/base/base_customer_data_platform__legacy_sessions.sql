{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "anonymous_id",
        "event_id"
    ],
) }}

/*
    The sessionization code is designed to break sessions when:
    1. UTM's change
    2. 30 minutes elapse (or midnight happens)
    3. When operating system changes

    anonymous_id as the Basis for Calculating Sessions:
        We are using anonymous_id as the foundation for session calculation because it uniquely identifies a 
        user or device prior to authentication or user identification. This approach ensures that we 
        accurately track user behavior across multiple visits and interactions, even before a user logs in 
        or provides identifiable information.

        When anonymous_id is not available, we fall back to user_id if available and then context_agent_name.
        This give us more changes to capture the session. If all else fails, there will be no session.
*/

WITH

legacy_events AS (
    SELECT * 
    FROM {{ ref('base_customer_data_platform__legacy_events') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, session_flags AS (
    SELECT
            *,
            /*
              First event
            */
            IF(event_sequence = 1, 'first', NULL) AS first_session,
            /*
                Flag time-boxed sessions:
                - When the prior event_at is >= 30 minutes ago
                - When there's no prior events
                - When it's a new day
            */
            CASE 
                WHEN TIMESTAMP_DIFF(event_at, last_event_at, MINUTE) >= 30
                    OR last_event_at IS NULL
                    OR DATE(event_at) > DATE(last_event_at)
                THEN 'timebox'
            END AS new_time_based_session,
            
            /*
                Flag campaign-based sessions:
                - When the prior campaign is different from the current one (not considering NULLs)
                - When the prior campaign is NULL
            */
            CASE 
                WHEN campaign != last_campaign
                    AND campaign IS NOT NULL
                    AND last_campaign IS NOT NULL
                    AND campaign NOT LIKE '%password_reset%'
                    AND last_campaign NOT LIKE '%password_reset%'
                THEN 'campaign-based'
            END AS new_campaign_based_session,
            
            /*
                Flag operating system-based sessions:
                - When the prior operating system is different from the current one (not considering NULLs)
                - When the prior operating system is NULL
            */
            CASE 
                WHEN context_os_name != last_os_name
                    AND context_os_name IS NOT NULL
                    AND last_os_name IS NOT NULL
                THEN 'system-based'
            END AS new_platform_based_session
        FROM legacy_events
)

, create_sessions AS (
    SELECT
        source_name
        , event_at
        , TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(DATE(event_at), INTERVAL 1 DAY)), INTERVAL 1 MILLISECOND) AS last_ts_of_day
        , COALESCE(anonymous_id, user_id, context_user_agent) AS session_user_id
        , CONCAT(
            source_name, '_', event_id, '_',
            COALESCE(first_session, new_time_based_session, new_campaign_based_session, new_platform_based_session)
        ) AS session_id
    FROM session_flags
    WHERE first_session IS NOT NULL
        OR new_time_based_session IS NOT NULL
        OR new_campaign_based_session IS NOT NULL
        OR new_platform_based_session IS NOT NULL
)

, session_bounds AS (
  SELECT
    session_id
    , session_user_id
    , source_name
    , event_at AS session_start_at
    , COALESCE(
      TIMESTAMP_SUB(LEAD(event_at) OVER (PARTITION BY session_user_id, source_name ORDER BY event_at), INTERVAL 1 MILLISECOND)
      , last_ts_of_day
    ) AS session_end_at
  FROM create_sessions
)

SELECT
    legacy_events.* EXCEPT(context_campaign_referrer, context_page_referrer, event_sequence)
    , session_bounds.session_id
    , session_bounds.session_user_id
    , session_bounds.session_start_at
    , session_bounds.session_end_at
	, {{ scrub_context_page_path('context_page_path') }}
    , COALESCE(legacy_events.context_page_referrer, legacy_events.context_campaign_referrer) AS context_page_referrer
    -- NET.REG_DOMAIN is a built-in BigQuery function to extract the domain from a URL
    , NET.REG_DOMAIN(legacy_events.context_page_path) AS context_page_referring_domain
    -- recreate the event_sequence now that we have the session_id
    , ROW_NUMBER() OVER (PARTITION BY session_bounds.session_id ORDER BY legacy_events.event_at) AS event_sequence
FROM legacy_events
LEFT JOIN session_bounds
    ON COALESCE(legacy_events.anonymous_id, legacy_events.user_id, legacy_events.context_user_agent) = session_bounds.session_user_id
    AND legacy_events.source_name = session_bounds.source_name
    AND legacy_events.event_at BETWEEN session_bounds.session_start_at AND session_bounds.session_end_at