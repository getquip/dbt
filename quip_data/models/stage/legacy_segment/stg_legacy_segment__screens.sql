-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-refresh
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


WITH

ios AS (
	SELECT * FROM {{ source('legacy_segment', 'ios__screens') }} t
)

, android_production AS (
	SELECT * FROM {{ source('legacy_segment', 'android_production__screens') }} t
)

, toothpic_prod_segment_mobile_quip_ios_prod AS (
	SELECT * FROM {{ source('legacy_segment', 'toothpic_prod_segment_mobile_quip_ios_prod__screens') }}
)

, toothpic_prod_segment_mobile_quip_android_prod AS (
	SELECT * FROM {{ source('legacy_segment', 'toothpic_prod_segment_mobile_quip_android_prod__screens') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

-- get relations
{%- set screens_sources = dbt_utils.get_relations_by_pattern(
	schema_pattern='legacy_segment'
	, table_pattern='%__screens'
	, database='quip-dw-raw'
) -%}

-- get columns from each relation
{%- set screens_columns = {} -%}
{% for relation in screens_sources %}
	{%- set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list | sort -%}
	{% set relation_name = relation.name | replace("__screens", '') %}
	{%- do screens_columns.update({relation_name: columns}) -%}
{% endfor %}

-- set columns to extract from sources
{% set model_columns = [
	'id'
	, 'user_id'
	, 'anonymous_id'
	, 'timestamp'
	, 'original_timestamp'
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
	, 'context_campaign_expid'
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
	, 'event'
	, 'received_at'
] %}

-- get columns to select from sources
{%- set select_columns = {} -%}
{% for relation, source_columns in screens_columns.items() %}
	{% set select_relation_columns = [] %}
	{%- do select_relation_columns.append("'" ~ relation ~ "' AS source_name") -%}
	
	{% for column in model_columns %}
		{% if column == 'id' %}
			{% do select_relation_columns.append(column ~ " AS event_id") %}
		{% elif column == 'context_page_path' and column in source_columns %}
			{% do select_relation_columns.append("CONCAT('/', TRIM(" ~ column ~ " , '/')) AS context_page_path") %}
		{% elif column == 'context_user_agent' and column in source_columns %}
			{% do select_relation_columns.append(column) %}
			{% do select_relation_columns.append("LOWER(" ~ column ~ ") AS device_info") %}
		{% elif column == 'event' and column in source_columns %}
			{% do select_relation_columns.append(column ~ " AS event_name") %}
		{% elif column in source_columns %}
			{% do select_relation_columns.append(column) %}
		{% else %}
			{% if column == 'context_screen_width' or column == 'context_screen_height' %}
				{% do select_relation_columns.append("CAST(NULL AS INT64) AS " ~ column) %}
			{% else %}
				{% if column == 'context_user_agent' %}
					{% do select_relation_columns.append("CAST(NULL AS STRING) AS device_info") %}
				{% endif %}
				{% do select_relation_columns.append("CAST(NULL AS STRING) AS " ~ column) %}
			{% endif %}
		{% endif %}
	{% endfor %}
	
	{%- do select_columns.update({relation: select_relation_columns}) -%}
{% endfor %}


, ios_screens AS (
	SELECT
		{{ select_columns['ios'] | join('\n		, ') }}
    FROM ios

	EXCEPT DISTINCT

	SELECT
		{{ select_columns['toothpic_prod_segment_mobile_quip_ios_prod'] | join('\n 		, ') }}
	FROM toothpic_prod_segment_mobile_quip_ios_prod
	WHERE received_at >= '2023-10-01'
)
, joined AS (
	SELECT * FROM ios_screens

	UNION ALL

	SELECT
		{{ select_columns['android_production'] | join('\n		 , ') }}
	FROM android_production

	UNION ALL

	SELECT
		{{ select_columns['toothpic_prod_segment_mobile_quip_android_prod'] | join('\n 		, ') }}
    FROM toothpic_prod_segment_mobile_quip_android_prod
)

, parsed AS (
	SELECT
		* EXCEPT(context_os_name, context_os_version, context_device_type, context_device_manufacturer)
		, context_os_name AS context_os_name_v1
		, context_os_version AS context_os_version_v1
		, context_device_type AS context_device_type_v1
		, context_device_manufacturer AS context_device_manufacturer_v1
		, {{ scrub_context_page_path('context_page_path') }}
		, {{ parse_device_info_from_user_agent('device_info') }}
		, IF(TIMESTAMP_DIFF(`timestamp`, original_timestamp, DAY) > 10, original_timestamp, `timestamp`) AS event_at
	FROM joined
)

SELECT
	* EXCEPT(context_os_name, context_os_version, context_device_type, context_device_manufacturer
		, context_os_name_v1, context_os_version_v1, context_device_type_v1, context_device_manufacturer_v1, event_at)
	, 'screens' AS event_type
	, context_library_name != 'analytics.js' AS is_server_side
	, COALESCE(context_os_name, context_os_name_v1) AS context_os_name
	, COALESCE(context_os_version, context_os_version_v1) AS context_os_version
	, IF(received_at < event_at, received_at, event_at) AS event_at
FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1