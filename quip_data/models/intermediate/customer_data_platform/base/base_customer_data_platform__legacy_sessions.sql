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
    pre_hook="{{ create_legacy_sessions() }}",
    post_hook="{{ drop_relations(session_events) }}"
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
*/

WITH

legacy_events AS (
    SELECT * 
    FROM {{ ref('base_customer_data_platform__legacy_event_context') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------


, created_sessions AS (
    {{ union_legacy_sessions() }}
)

SELECT
    legacy_events.*
    , created_sessions.session_id
	, {{ scrub_context_page_path('context_page_path') }}
    , COALESCE(context_page_referrer, context_campaign_referrer) AS context_page_referrer
    -- NET.REG_DOMAIN is a built-in BigQuery function to extract the domain from a URL
    , NET.REG_DOMAIN(legacy_events.context_page_path) AS context_page_referring_domain
FROM legacy_events
LEFT JOIN created_sessions
    ON legacy_events.event_id = created_sessions.event_id
    AND legacy_events.source_name = created_sessions.source_name