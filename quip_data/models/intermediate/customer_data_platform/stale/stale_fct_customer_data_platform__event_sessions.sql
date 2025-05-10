
{{ config(
    materialized='table',
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

 
{% set relations = [
	source('legacy_segment', 'stg_legacy_segment__tracks')
	, source('littledata', 'stg_littledata__tracks')
	, source('legacy_segment', 'stg_legacy_segment__pages')
	, source('littledata', 'stg_littledata__pages')
	, source('legacy_segment', 'stg_legacy_segment__screens')
] %}

{% set incremental_clause = None %}

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
WITH

events_base AS ( -- union of all events

    {% set model_columns = [
        ('event_id', 'STRING')
        , ('user_id', 'STRING')
        , ('anonymous_id', 'STRING')
        , ('event_at', 'TIMESTAMP')
        , ('event_name', 'STRING')
        , ('context_campaign_content', 'STRING')
        , ('context_campaign_medium', 'STRING')
        , ('context_campaign_name', 'STRING')
        , ('context_campaign_source', 'STRING')
        , ('context_campaign_term', 'STRING')
        , ('context_ip', 'STRING')
        , ('context_locale', 'STRING')
        , ('context_page_path', 'STRING')
        , ('context_page_referrer', 'STRING')
        , ('context_page_search', 'STRING')
        , ('context_page_title', 'STRING')
        , ('context_page_url', 'STRING')
        , ('context_user_agent', 'STRING')
        , ('context_campaign_type', 'STRING')
        , ('context_campaign_referrer', 'STRING')
        , ('context_campaign_id', 'STRING')
        , ('context_library_name', 'STRING')
        , ('context_library_version', 'STRING')
        , ('context_app_version', 'STRING')
        , ('context_device_manufacturer', 'STRING')
        , ('context_device_type', 'STRING')
        , ('context_os_name', 'STRING')
        , ('context_os_version', 'STRING')
        , ('context_screen_height', 'NUMERIC')
        , ('context_screen_width', 'NUMERIC')
        , ('received_at', 'TIMESTAMP')
        , ('source_name', 'STRING')
        , ('event_type', 'STRING')
        , ('browser_category', 'STRING')
        , ('browser_name', 'STRING')
        , ('browser_vendor', 'STRING')
    ] %}

    {{ union_different_relations(relations, model_columns, incremental_clause) }}
)

, events_sequenced AS (
    SELECT
        events_base.*
        , CONCAT(
            context_campaign_source
            , context_campaign_medium
            , context_campaign_name
            , context_campaign_content
            , context_campaign_term
        ) AS campaign
        , ROW_NUMBER() OVER (PARTITION BY anonymous_id, source_name ORDER BY event_at, event_id) AS event_sequence
    FROM events_base
)

, events AS (
    SELECT
        curr.*
        , prev.campaign AS last_campaign
        , prev.context_os_name AS last_os_name
        , prev.event_at AS last_event_at
    FROM events_sequenced AS curr
    LEFT JOIN events_sequenced AS prev
        ON curr.anonymous_id = prev.anonymous_id
        AND curr.source_name = prev.source_name
        AND curr.event_sequence = prev.event_sequence + 1
)


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

, session_flags AS (
    SELECT
            *,
            /* First event */
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
        FROM events
)

, create_sessions AS (
    SELECT
        source_name
        , event_at AS session_start_at
        , COALESCE(
            TIMESTAMP_SUB(LEAD(event_at) OVER 
                (PARTITION BY COALESCE(anonymous_id, user_id, context_user_agent), source_name ORDER BY event_at), INTERVAL 1 MILLISECOND)
            , TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(DATE(event_at), INTERVAL 1 DAY)), INTERVAL 1 MILLISECOND)
         ) AS session_end_at
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

SELECT
    events.* EXCEPT(context_campaign_referrer, context_page_referrer, event_sequence)
    , create_sessions.session_id
    , create_sessions.session_user_id
    , create_sessions.session_start_at
    , create_sessions.session_end_at
    , COALESCE(events.context_page_referrer, events.context_campaign_referrer) AS context_page_referrer
    -- NET.REG_DOMAIN is a built-in BigQuery function to extract the domain from a URL
    , NET.REG_DOMAIN(events.context_page_path) AS context_page_referring_domain
    -- recreate the event_sequence now that we have the session_id
    , ROW_NUMBER() OVER (PARTITION BY create_sessions.session_id ORDER BY events.event_at) AS event_sequence
    , {{ scrub_context_page_path('context_page_path') }}
FROM events
LEFT JOIN create_sessions
    ON COALESCE(events.anonymous_id, events.user_id, events.context_user_agent) = create_sessions.session_user_id
    AND events.source_name = create_sessions.source_name
    AND events.event_at BETWEEN create_sessions.session_start_at AND create_sessions.session_end_at