{{ config(
    materialized='table',
    partition_by={
        "field": "event_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
		"event_name",
        "anonymous_id",
        "shopify_customer_id",
        "event_id"
    ]
) }}


WITH

events AS (
	SELECT * FROM {{ ref('int_fct_customer_data_platform__events') }}
)

, context AS (
	SELECT * FROM {{ ref('int_fct_customer_data_platform__event_context') }}
)

, users AS (
	SELECT * FROM {{ ref('int_dim_customer_data_platform__session_users') }}
)

, session_dims AS (
	SELECT * FROM {{ ref('int_dim_customer_data_platform__sessions') }}
)


-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	events.* EXCEPT(user_id, context_device_type, context_user_agent
		, context_os_name, context_app_version, browser_category, browser_name, browser_vendor)
	, users.shopify_customer_id
	, context.* EXCEPT(event_id, source_name, user_id, anonymous_id, event_at, event_name, event_type)
	, session_dims.* EXCEPT(source_name, anonymous_id, session_id)
	, CASE
        WHEN events.context_library_name IN ('analytics-next', 'RudderLabs JavaScript SDK', 'analytics.js', 'analytics-ios', 'analytics-android', 'analytics-kotlin') THEN 'client-side'
        WHEN events.context_library_name IN ('analytics-ruby', '@segment/analytics-node', 'RudderStack Shopify Cloud') THEN 'server-side'
        ELSE 'unknown'
    END AS event_category
	, CASE
		WHEN events.source_name IN ('ios', 'android_production', 'toothpic_prod_segment_mobile_quip_ios_prod', 'toothpic_prod_segment_mobile_quip_android_prod') THEN 'app'
	END AS platform
FROM events
LEFT JOIN context
	ON events.event_id = context.event_id
	AND events.source_name = context.source_name
LEFT JOIN users
	ON events.session_id = users.session_id
	AND events.source_name = users.source_name
LEFT JOIN session_dims
	ON events.session_id = session_dims.session_id
	AND events.source_name = session_dims.source_name