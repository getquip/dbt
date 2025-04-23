-- depends_on: {{ ref('base_customer_data_platform__legacy_sessions') }}

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

{% set model_columns = [
	('event_id', 'STRING')
	, ('user_id', 'STRING')
	, ('session_id', 'STRING')
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
	, ('context_page_path_scrubbed', 'STRING')
	, ('context_page_referrer', 'STRING')
	, ('context_page_referring_domain', 'STRING')
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

{% set relations = [
    ref('stg_rudderstack__tracks') 
    , ref('stg_rudderstack__pages') 
] %}

{% if not is_incremental() %}
    {% do relations.append(ref('base_customer_data_platform__legacy_sessions')) %}
	{% set incremental_clause = None %}
{% else %}
	{% set incremental_clause = "event_at >= '" ~ get_max_partition('event_at', lookback_window=30) ~ "'" %}
{% endif %}


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
WITH

events AS (
	{{ union_different_relations(relations, model_columns, incremental_clause) }}
)


, page_event_sequence AS (
	SELECT
		events.event_id
		, events.session_id
		, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS page_event_sequence
		, TIMESTAMP_DIFF(LEAD(event_at) OVER 
			(PARTITION BY session_id, context_page_path_scrubbed ORDER BY event_at), event_at, SECOND) AS page_time_spent_seconds
	FROM events
	WHERE event_type IN ('page', 'screen')
)

, track_event_sequence AS (
	SELECT
		events.event_id
		, events.session_id
		, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS track_event_sequence
	FROM events
	WHERE event_type = 'track'
)

SELECT
	events.*
	, pages.page_event_sequence
	, pages.page_time_spent_seconds
	, tracks.track_event_sequence
	, ROW_NUMBER() OVER (PARTITION BY events.session_id ORDER BY events.event_at, events.event_id) AS event_sequence
FROM events
LEFT JOIN page_event_sequence AS pages
	ON events.event_id = pages.event_id
LEFT JOIN track_event_sequence AS tracks
	ON events.event_id = tracks.event_id