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
    FROM {{ ref('base_customer_data_platform__legacy_events') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------



, created_sessions AS (
    -- get relations
    {%- set session_events = dbt_utils.get_relations_by_pattern(
        schema_pattern = this.schema,
        table_pattern = 'base_customer_data_platform__legacy_sessions_%',
        database = this.database
    ) -%}

    {% do log(session_events, info=True) %}

    {% for events in session_events %}
        
        SELECT * FROM {{ events }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    legacy_events.*
    , created_sessions.session_id
FROM legacy_events
LEFT JOIN created_sessions
    ON legacy_events.event_id = created_sessions.event_id
    AND legacy_events.source_name = created_sessions.source_name