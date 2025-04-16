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
	'event_id'
	, 'user_id'
	, 'anonymous_id'
	, 'event_at'
	, 'event_name'
	, 'context_campaign_content'
	, 'context_campaign_medium'
	, 'context_campaign_name'
	, 'context_campaign_source'
	, 'context_campaign_term'
	, 'context_ip'
	, 'context_locale'
	, 'context_page_path'
	, 'context_page_referrer'
	, 'context_page_search'
	, 'context_page_title'
	, 'context_page_url'
	, 'context_user_agent'
	, 'context_campaign_type'
	, 'context_campaign_referrer'
	, 'context_campaign_id'
	, 'context_library_name'
	, 'context_library_version'
	, 'context_app_version'
	, 'context_device_manufacturer'
	, 'context_device_type'
	, 'context_os_name'
	, 'context_os_version'
	, 'context_screen_height'
	, 'context_screen_width'
	, 'received_at'
	, 'source_name'
	, 'event_type'
	, 'browser_category'
	, 'browser_name'
	, 'browser_vendor'
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
	{% set queries = [] %}
	{% for relation in relations %}
		-- get columns from each relation
		{%- set relation_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list | sort -%}

		-- fill in nulls
		{% set select_columns = [] %}
		{% for column in model_columns %}
			{%- if column not in relation_columns -%}
				{% do select_columns.append("CAST(NULL AS STRING) AS " ~ column) %}
			{% else %}
				{% do select_columns.append(column) %}
			{% endif %}
		{% endfor %}

		-- create select statement
		{% set query %}
		SELECT 
			{{ select_columns | join(',\n\t') }} 
			, CONCAT(
				context_campaign_source
				, context_campaign_medium
				, context_campaign_name
				, context_campaign_content
				, context_campaign_term
			) AS campaign
		FROM {{ relation }}
		{% endset %}

		{% do queries.append(query) %}
	{% endfor %}

	{{ queries | join('\nUNION ALL\n') }}
)

, event_base AS (
	SELECT
		*
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