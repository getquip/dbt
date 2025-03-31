WITH

events AS (
	SELECT * FROM {{ ref('int_dim_customer_data_platform__events') }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	session_id
	, context_device_type
	, context_user_agent
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
	, REGEXP_REPLACE(
            device_info,
            r"spider|crawler|googlebot|storebot-google|google-read-aloud|facebookexternalhit|facebookcatalog|bingbot|pinterestbot|applebot|yandexmobilebot|petalbot|duckduckbot|yandexbot|bublupbot|cocolyzebot|facebot|twitterbot|klarnapricewatcherbot|klarnabot|semrushbot"
    ) AS is_bot
	, COUNT(event_id) AS count_all_events
	, COUNT(DISTINCT context_page_path_scrubbed) AS count_unique_page_paths
	, MIN(event_at) AS session_start_at
	, MAX(event_at) AS session_end_at
	, TIMESTAMP_DIFF(MAX(event_at), MIN(event_at), SECOND) AS session_duration_seconds
FROM events