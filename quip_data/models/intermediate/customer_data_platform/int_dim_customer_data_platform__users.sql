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
        "identifies_event_id"
    ]
) }}

WITH

identifies AS (
	SELECT * FROM {{ ref('int_fct_customer_data_platform__identifies') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, known_users AS (
	SELECT DISTINCT
		user_id
		, anonymous_id
	FROM identifies
	WHERE user_id IS NOT NULL
)

SELECT DISTINCT
	identifies.anonymous_id
	, COALESCE(identifies.user_id, known_users.user_id) AS user_id
FROM identifies
INNER JOIN known_users
	ON identifies.anonymous_id = known_users.anonymous_id