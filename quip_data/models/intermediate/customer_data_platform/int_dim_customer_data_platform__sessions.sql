{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
        "field": "session_start_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "source_name",
        "context_device_type", 
        "anonymous_id",
        "session_id"
    ]
) }}

WITH

events AS (
	SELECT * FROM {{ ref('int_fct_customer_data_platform__events') }}
	{% if is_incremental() %}
		WHERE event_at >= "{{ get_max_partition('session_start_at', lookback_window=30) }}"
	{% endif %}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------
, dimensions AS (
	SELECT
		session_id
		, anonymous_id
		, source_name
		, context_device_type
		, context_user_agent
		, context_os_name 
		, context_app_version
		, browser_category = 'crawler' AS is_bot
		, browser_category
		, browser_name
		, browser_vendor	
		, context_page_path AS session_landing_page_path
		, context_page_title AS session_landing_page_title
		, context_page_url AS session_landing_page_url
		, context_page_search AS session_landing_page_search
		, context_page_referrer AS session_landing_page_referrer
		, context_page_referring_domain AS session_landing_page_referrer_domain
		, context_campaign_id AS session_landing_page_campaign_id
		, context_campaign_name AS session_landing_page_campaign_name
		, context_campaign_source AS session_landing_page_campaign_source
		, context_campaign_medium AS session_landing_page_campaign_medium
		, context_campaign_content AS session_landing_page_campaign_content
		, REGEXP_EXTRACT(context_page_url, '[?&]coupon=([^&]+)') AS session_landing_page_coupon_code	
	FROM events
	WHERE event_sequence = 1
)

, aggregated_events AS (
	SELECT
	session_id
	, COUNTIF(event_type IN ('page', 'screen')) AS count_page_events
	, COUNTIF(event_type = 'track'
		-- We don't want to count non-interaction events, like viewing a product or hovering
		-- as track events in a session, which will be used to calculate bounce rate 
		AND context_page_path_scrubbed NOT LIKE '%viewed%'
		AND context_page_path_scrubbed NOT LIKE '%hover%'
		AND context_page_path_scrubbed NOT LIKE '%scrolled%'
		AND context_page_path_scrubbed NOT LIKE '%application_backgrounded%'
		AND context_page_path_scrubbed NOT LIKE '%application_opened%'
		AND context_page_path_scrubbed NOT LIKE '%experiment%'
	) AS count_track_events
	, COUNT(event_id) AS count_all_events
	, COUNT(DISTINCT context_page_path_scrubbed) AS count_unique_page_paths
	, MIN(event_at) AS session_start_at
	, MAX(event_at) AS session_end_at
	, TIMESTAMP_DIFF(MAX(event_at), MIN(event_at), SECOND) AS session_duration_seconds
FROM events
GROUP BY 1
)

SELECT
	dimensions.*
	, aggregated_events.* EXCEPT(session_id)
FROM dimensions
INNER JOIN aggregated_events
	ON dimensions.session_id = aggregated_events.session_id