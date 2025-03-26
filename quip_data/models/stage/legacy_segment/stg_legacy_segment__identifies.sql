-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-refresh
{{ config(
    materialized='table',
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "user_id", 
        "anonymous_id",
        "identifies_id"
    ]
) }}


WITH

quip_production AS (
	SELECT * FROM {{ source("legacy_segment", "quip_production__identifies") }}
)

, ios AS (
	SELECT * FROM {{ source('legacy_segment', 'ios__identifies') }} t
)

, android_production AS (
	SELECT * FROM {{ source('legacy_segment', 'android_production__identifies') }} t
)

, toothpic_prod_segment_mobile_quip_ios_prod AS (
	SELECT * FROM {{ source('legacy_segment', 'toothpic_prod_segment_mobile_quip_ios_prod__identifies') }}
)

, toothpic_prod_segment_mobile_quip_android_prod AS (
	SELECT * FROM {{ source('legacy_segment', 'toothpic_prod_segment_mobile_quip_android_prod__identifies') }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, cleaned AS (
	SELECT
		"legacy" AS source_name
		, `name` AS address_first_name
		, `name` AS address_last_name
		, `name` AS address_name
		, anonymous_id
		, context_campaign_content
		, context_campaign_creative
		, context_campaign_device
		, context_campaign_medium
		, context_campaign_name
		, context_campaign_source
		, context_campaign_term
		, context_ip
		, context_library_name
		, context_library_version
		, context_locale
		, context_page_path
		, context_page_referrer
		, context_page_search
		, context_page_title
		, context_page_url
		, context_timezone
		, context_user_agent
		, email
		, `name` AS first_name
		, id AS identifies_id
		, `name` AS last_name
		, loaded_at
		, original_timestamp
		, received_at
		, sent_at
		, context_campaign_tags AS tags
		, `timestamp`
		, user_id
		, uuid_ts
	FROM quip_production
)

SELECT 
	* 
	, 'track' as event_type
	, IF(context_library_name = '@segment/analytics-node', 'backend', 'web') AS platform
	, {{ scrub_context_page_path('context_page_path') }} 
	, {{ create_touchpoint('context_page_path') }}
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY identifies_id ORDER BY received_at DESC ) = 1