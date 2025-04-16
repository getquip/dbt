-- this data is stale, this model should only be used for historical purposes.
-- this model should only be run during a --full-sourceresh
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

{% set sources = [
	source("legacy_segment", 'ios__tapped')
	, source("legacy_segment", 'toothpic_prod_segment_mobile_quip_ios_prod__tapped')
	, source("legacy_segment", 'android_production__tapped')
	, source("legacy_segment", 'toothpic_prod_segment_mobile_quip_android_prod__tapped')
] %}

{% set model_columns = [
    'id'
    , 'user_id'
    , 'anonymous_id'
    , 'timestamp'
    , 'target_location'
    , 'target_text'
    , 'target_type'
    , 'target_name'
    , 'received_at'
    , 'event'
] %}

WITH

joined AS (
    {{ dbt_utils.union_relations(
        relations=sources
        , include=model_columns
        , source_column_name='source_name'
    ) }}
)

SELECT
	id AS event_id
	, `timestamp` AS event_at
	, target_location
	, target_text
	, target_type
    , target_name
    , REPLACE(source_name, '__tapped', '') AS source_name
    , user_id
    , anonymous_id
	, `event` AS event_name
FROM joined
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_at DESC) = 1