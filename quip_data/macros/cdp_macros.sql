{% macro scrub_context_page_path(context_page_path) %}
	--this removes any unique identifiers from the page path
	{% set query %}
		REGEXP_REPLACE(
			{{ context_page_path}}
			, r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})|(sub_[a-z0-9]+)|([0-9]{6,})'
				, "<< removed >>"
		) AS context_page_path_scrubbed
	{% endset %}

	{% do return(query) %}
{% endmacro %}
----------------------------------------------------------------------------------------------------
{% macro parse_device_info_from_user_agent(user_agent) %}

  -- Parse Device Manufacturer (in lowercase)
  {% set device_manufacturer %}
    CASE
      WHEN {{ user_agent }} LIKE '%sm-%' 
        OR {{ user_agent }} LIKE '%samsung%' 
        THEN 'samsung'
      WHEN {{ user_agent }} LIKE '%iphone%' 
        OR {{ user_agent }} LIKE '%ipad%' 
        OR {{ user_agent }} LIKE '%ipod%' 
        OR {{ user_agent }} LIKE '%mac%'
        THEN 'apple'
      WHEN {{ user_agent }} LIKE '%pixel%' 
        OR {{ user_agent }} LIKE '%nexus%' 
        THEN 'google'
      WHEN {{ user_agent }} LIKE '%huawei%' 
        OR {{ user_agent }} LIKE '%honor%' 
        THEN 'huawei'
      WHEN {{ user_agent }} LIKE '%lg-%' 
        THEN 'lg'
      WHEN {{ user_agent }} LIKE '%xiaomi%' 
        THEN 'xiaomi'
    END
  {% endset %}
  
  -- Parse Operating System (in lowercase)
  {% set operating_system %}
    CASE
      WHEN {{ user_agent }} LIKE '%android%' 
        THEN 'android'
      WHEN {{ user_agent }} LIKE '%ios%' 
        OR {{ user_agent }} LIKE '%iphone%' 
        OR {{ user_agent }} LIKE '%ipad%' 
        THEN 'ios'
      WHEN {{ user_agent }} LIKE '%windows%' 
        THEN 'windows'
      WHEN {{ user_agent }} LIKE '%mac%' 
        OR {{ user_agent }} LIKE '%macintosh%' 
        THEN 'macos'
      WHEN {{ user_agent }} LIKE '%linux%' 
        THEN 'linux'
      WHEN {{ user_agent }} LIKE '%chrome%' 
        AND {{ user_agent }} LIKE '%mobile%' 
        THEN 'chrome os (mobile)'
      WHEN {{ user_agent }} LIKE '%chrome%' 
        THEN 'chrome os (desktop)'
    END
  {% endset %}

  {% set operating_system_version %}
    CASE
      WHEN {{ user_agent }} LIKE '%android%' 
        THEN 'android ' || REGEXP_EXTRACT({{ user_agent }}, 'android ([^; ]+)')
      WHEN {{ user_agent }} LIKE '%ios%' 
        OR {{ user_agent }} LIKE '%iphone%' 
        OR {{ user_agent }} LIKE '%ipad%' 
        THEN 'ios ' || REGEXP_EXTRACT({{ user_agent }}, 'os ([^ ]+)')
      WHEN {{ user_agent }} LIKE '%windows%' 
        THEN 'windows ' || REGEXP_EXTRACT({{ user_agent }}, 'windows nt ([^; ]+)')
      WHEN {{ user_agent }} LIKE '%mac%' 
        OR {{ user_agent }} LIKE '%macintosh%' 
        THEN 'macos ' || REGEXP_EXTRACT({{ user_agent }}, 'mac os x ([^ ;]+)')
      WHEN {{ user_agent }} LIKE '%linux%' 
        THEN 'linux ' || REGEXP_EXTRACT({{ user_agent }}, 'linux ([^; ]+)')
      WHEN {{ user_agent }} LIKE '%chrome%' 
        AND {{ user_agent }} LIKE '%mobile%' 
        THEN 'chrome os (mobile)' || REGEXP_EXTRACT({{ user_agent }}, 'chrome/([0-9]+)')
      WHEN {{ user_agent }} LIKE '%chrome%' 
        THEN 'chrome os (desktop)' || REGEXP_EXTRACT({{ user_agent }}, 'chrome/([0-9]+)')
    END
  {% endset %}

  {% set device_type %}
    CASE
      WHEN {{ user_agent }} LIKE '%android%'
		    OR {{ user_agent }} LIKE '%iphone%' 
		    OR {{ user_agent }} LIKE '%ios%' 
		    OR {{ user_agent }} LIKE '%mobile%'
        OR {{ user_agent }} LIKE '%sm-g%' -- samsung galaxy series
		    THEN 'mobile'
      WHEN {{ user_agent }} LIKE '%windows%' 
	  	  OR {{ user_agent }} LIKE '%macintosh%'
		    OR {{ user_agent }} LIKE '%mac%'
		    OR {{ user_agent }} LIKE '%desktop%'
        OR {{ user_agent }} LIKE '%chrome%'
	  	  THEN 'computer/desktop'
      WHEN {{ user_agent }} LIKE '%sm-t%' -- samsung tab series
        OR {{ user_agent }} LIKE '%pixel c%'
        OR {{ user_agent }} LIKE '%nexus 7%'
        OR {{ user_agent }} LIKE '%tablet%'
        OR {{ user_agent }} LIKE '%ipad%'
        THEN 'tablet'
    END
  {% endset %}

 -- Return the parsed values
    {{ device_manufacturer }} AS context_device_manufacturer
    , {{ operating_system }} AS context_os_name
	  , {{ operating_system_version }} AS context_os_version
    , {{ device_type }} AS context_device_type
{% endmacro %}
----------------------------------------------------------------------------------------------------
{% macro create_legacy_sessions() %}
  -- get time frames
  {% set time_query %}
    SELECT
      CONCAT("BETWEEN '", date, "' AND '", DATE_ADD(date, INTERVAL 20 DAY), "'") AS frames
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2025-03-01', INTERVAL 3 WEEK)) AS date
    ORDER BY 1
  {% endset %}

  {% set time_result = dbt_utils.get_query_results_as_dict(time_query) %}
  
  {% set time_frames = ["< '2020-01-01'"] %}
  {% for row in time_result['frames'] %}
    {% do time_frames.append(row) %}
  {% endfor %}

  -- create session relations
  {% for frame in time_frames %}
    {% set relation_name = this.database ~ '.' ~ this.schema ~ '.base_customer_data_platform__legacy_sessions_' ~ loop.index %}
    {% do log("Creating session relation: " ~ relation_name, info=True) %}
    {% set query %}
      CREATE OR REPLACE TABLE {{ relation_name }} AS

      WITH RECURSIVE legacy_events AS (
          SELECT *
          FROM {{ ref("base_customer_data_platform__legacy_events") }}
          WHERE event_at {{ frame }}
      )
      
      , session_flags AS (
          SELECT
              *,
              /*
                  Flag time-boxed sessions:
                  - When the prior event_at is >= 30 minutes ago
                  - When there's no prior events
                  - When it's a new day
              */
              CASE 
                  WHEN TIMESTAMP_DIFF(event_at, last_event_at, MINUTE) >= 30
                      OR last_event_at IS NULL
                      OR DATE(event_at) > DATE(last_event_at)
                  THEN 1 
                  ELSE 0 
              END AS new_time_based_session,
              
              /*
                  Flag campaign-based sessions:
                  - When the prior campaign is different from the current one (not considering NULLs)
                  - When the prior campaign is NULL
              */
              CASE 
                  WHEN campaign != last_campaign
                      AND campaign IS NOT NULL
                      AND last_campaign IS NOT NULL
                      AND campaign NOT LIKE '%password_reset%'
                      AND last_campaign NOT LIKE '%password_reset%'
                  THEN 1 
                  ELSE 0 
              END AS new_campaign_based_session,
              
              /*
                  Flag operating system-based sessions:
                  - When the prior operating system is different from the current one (not considering NULLs)
                  - When the prior operating system is NULL
              */
              CASE 
                  WHEN context_os_name != last_os_name
                      AND context_os_name IS NOT NULL
                      AND last_os_name IS NOT NULL
                  THEN 1 
                  ELSE 0 
              END AS new_platform_based_session
          FROM legacy_events
      )
      
      , create_sessions AS (
          SELECT
              event_id,
              event_sequence,
              anonymous_id,
              source_name,
              event_at,
              CONCAT(
                  anonymous_id,
                  '_',
                  CAST(
                      SUM(GREATEST(new_time_based_session, new_campaign_based_session, new_platform_based_session)) 
                        OVER (PARTITION BY anonymous_id, source_name ORDER BY event_at, event_id) 
                      AS STRING
                  )
              ) AS session_id
          FROM session_flags
      )

      SELECT * FROM create_sessions
    {% endset %}

    {% do run_query(query) %}
    {% set relation_name = this.database ~ '.' ~ this.schema ~ '.base_customer_data_platform__legacy_sessions_' ~ loop.index %}
  {% endfor %}
  
  {% do log("Finished creating session relations", info=True) %}
{% endmacro %}
