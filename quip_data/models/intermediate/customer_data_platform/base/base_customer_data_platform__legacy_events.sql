{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "last_event_at",
        "context_os_name", 
        "anonymous_id",
		"event_id"
    ]
) }}

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


{% set relations = [
	ref('stg_legacy_segment__tracks')
	, ref('stg_littledata__tracks')
	, ref('stg_legacy_segment__pages')
	, ref('stg_littledata__pages')
	, ref('stg_legacy_segment__screens')
] %}

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
WITH 

events AS (
	{{ union_different_relations(relations, model_columns) }}
)

, event_base AS (
	SELECT
		*
		, CONCAT(
				context_campaign_source
				, context_campaign_medium
				, context_campaign_name
				, context_campaign_content
				, context_campaign_term
			) AS campaign
		, ROW_NUMBER() OVER (PARTITION BY anonymous_id, source_name ORDER BY event_at, event_id) AS event_sequence
	FROM events
)

SELECT
	curr.*
	, prev.campaign AS last_campaign
	, prev.context_os_name AS last_os_name
	, prev.event_at AS last_event_at
FROM event_base AS curr
LEFT JOIN event_base AS prev
	ON curr.anonymous_id = prev.anonymous_id
	AND curr.source_name = prev.source_name
	AND curr.event_sequence = prev.event_sequence + 1