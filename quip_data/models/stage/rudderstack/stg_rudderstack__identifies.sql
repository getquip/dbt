{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by={
        "field": "source_synced_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "house_bill_number",
        "shipment_type", 
        "invoice_number",
        "invoice_line_item_id"
    ]
) }}


WITH

source AS (
	SELECT * FROM {{ source('rudderstack_prod', 'identifies') }}
)

, historical__littledata AS (
	SELECT * FROM {{ source("segment", "littledata__identifies") }}
)

, historical__legacy AS (
	SELECT * FROM {{ source("segment", "legacy__identifies") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS (
	SELECT
		id AS event_id
		, anonymous_id
		, user_id AS shopify_user_id
		, event AS event_name
		, created_at
		, updated_at
		, received_at
		, sent_at
		, processed_at
		, loaded_at AS source_synced_at
		, original_timestamp
		, timestamp AS event_timestamp
		, source_name AS event_source_name
		, test AS is_test
	FROM source

	{% if is_incremental() %}
		UNION ALL
		
		SELECT
			id AS event_id
			, anonymous_id
			, user_id AS shopify_user_id
			, event AS event_name
			, created_at
			, updated_at
			, received_at
			, sent_at
			, processed_at
			, loaded_at AS source_synced_at
			, original_timestamp
			, timestamp AS event_timestamp
			, source_name AS event_source_name
			, test AS is_test
		FROM historical__littledata

		UNION ALL

			SELECT
			id AS event_id
			, anonymous_id
			, user_id AS shopify_user_id
			, event AS event_name
			, created_at
			, updated_at
			, received_at
			, sent_at
			, processed_at
			, loaded_at AS source_synced_at
			, original_timestamp
			, timestamp AS event_timestamp
			, source_name AS event_source_name
			, test AS is_test
		FROM historical__legacy
	{% endif %}
)


SELECT * FROM cleaned
WHERE NOT is_test
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY source_synced_at DESC ) = 1