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
	events.* EXCEPT(user_id, session_id)
	, users.shopify_customer_id AS user_id
	, context.* EXCEPT(event_id, source_name, user_id, anonymous_id)
	, session_dims.* EXCEPT(event_id, source_name, user_id, anonymous_id, session_id)
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