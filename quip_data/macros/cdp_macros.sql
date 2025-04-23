{% macro scrub_context_page_path(context_page_path) %}
	--this removes any unique identifiers from the page path
		REGEXP_REPLACE(
			{{ context_page_path}}
			, r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})|(sub_[a-z0-9]+)|([0-9]{6,})'
				, "<< removed >>"
		) AS context_page_path_scrubbed
{% endmacro %}
----------------------------------------------------------------------------------------------------
{% macro parse_server_side_event(context_library_name) %}

  CASE
    WHEN {{ context_library_name }} IN ('analytics-ruby', '@segment/analytics-node', 'RudderStack Shopify Cloud')
      THEN 'server-side'
    WHEN {{ context_library_name }} IN ('analytics-ios', 'analytics.js', 'analytics-android', 'analytics-kotlin', 'RudderLabs JavaScript SDK')
      THEN 'client-side'
    ELSE 'unknown'
  END AS call_type
	
{% endmacro %}
----------------------------------------------------------------------------------------------------
{% macro parse_device_info_from_user_agent(user_agent) %}
/*
  Sanitization explained:
    Remove common browser identifiers to avoid false positives in parsing.

    - mozilla/5.0 : is a legacy identifier included in modern user-agent strings in order to maintain backwards compatability.
    - khtml, like gecko: Modern browsers use this retain this for backward compatibility
    - version/xx.xx : refers to the WebView version or rendering engine rather than the browser itself
    - wv: WebView events often mimic browser behaviors but do not represent standalone browsers
    - applewebkit/xx.xx : many modern browsers are built on WebKit or Blink, this information does not distinguish different browser types.
    - safari/xx.xx : Many non-Safari browsers (e.g., Chrome on iOS) include this string to maintain compatibility.
*/
  {% set sanitized_user_agent = "REGEXP_REPLACE(" ~ user_agent ~ ", 
        r'(mozilla/5\.0|khtml, like gecko|version/\d+\.\d+|wv|applewebkit/\d+\.\d+|safari/\d+\.\d+|like mac os x)', 
        ''
    )" %}
  -- Parse Device Manufacturer
    CASE
      WHEN {{ sanitized_user_agent }} LIKE '%sm-%' 
        OR {{ sanitized_user_agent }} LIKE '%samsung%' 
        THEN 'samsung'
      WHEN {{ sanitized_user_agent }} LIKE '%iphone%' 
        OR {{ sanitized_user_agent }} LIKE '%ipad%' 
        OR {{ sanitized_user_agent }} LIKE '%ipod%' 
        OR {{ sanitized_user_agent }} LIKE '%mac%'
        THEN 'apple'
      WHEN {{ sanitized_user_agent }} LIKE '%pixel%' 
        OR {{ sanitized_user_agent }} LIKE '%nexus%' 
        THEN 'google'
      WHEN {{ sanitized_user_agent }} LIKE '%huawei%' 
        OR {{ sanitized_user_agent }} LIKE '%honor%' 
        THEN 'huawei'
      WHEN {{ sanitized_user_agent }} LIKE '%lg-%' 
        THEN 'lg'
      WHEN {{ sanitized_user_agent }} LIKE '%xiaomi%' 
        THEN 'xiaomi'
      WHEN {{ sanitized_user_agent }} LIKE '%bingsapphire%' 
        THEN 'microsoft'
    END AS context_device_manufacturer
  
  -- Parse Operating System
    , CASE
      WHEN {{ sanitized_user_agent }} LIKE '%android%' 
        THEN 'android'
      WHEN {{ sanitized_user_agent }} LIKE '%ios%' 
        OR {{ sanitized_user_agent }} LIKE '%iphone%' 
        OR {{ sanitized_user_agent }} LIKE '%ipad%' 
        THEN 'ios'
      WHEN {{ sanitized_user_agent }} LIKE '%windows%' 
        THEN 'windows'
      WHEN {{ sanitized_user_agent }} LIKE '%mac%' 
        OR {{ sanitized_user_agent }} LIKE '%macintosh%' 
        THEN 'macos'
      WHEN {{ sanitized_user_agent }} LIKE '%linux%' 
        THEN 'linux'
      WHEN {{ sanitized_user_agent }} LIKE '%chrome%' 
        AND {{ sanitized_user_agent }} LIKE '%mobile%' 
        THEN 'chrome os (mobile)'
      WHEN {{ sanitized_user_agent }} LIKE '%chrome%' 
        THEN 'chrome os (desktop)'
    END AS context_os_name

  -- Parse Operating System Version
    , CASE
      WHEN {{ sanitized_user_agent }} LIKE '%android%' 
        THEN 'android ' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'android ([^; ]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%ios%' 
        OR {{ sanitized_user_agent }} LIKE '%iphone%' 
        OR {{ sanitized_user_agent }} LIKE '%ipad%' 
        THEN 'ios ' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'os ([^ ]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%windows%' 
        THEN 'windows ' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'windows nt ([^; ]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%mac%' 
        OR {{ sanitized_user_agent }} LIKE '%macintosh%' 
        THEN 'macos ' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'mac os x ([^ ;]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%linux%' 
        THEN 'linux ' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'linux ([^; ]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%chrome%' 
        AND {{ sanitized_user_agent }} LIKE '%mobile%' 
        THEN 'chrome os' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'chrome/([0-9]+)')
      WHEN {{ sanitized_user_agent }} LIKE '%chrome%' 
        THEN 'chrome os' || REGEXP_EXTRACT({{ sanitized_user_agent }}, 'chrome/([0-9]+)')
    END AS context_os_version
  
  -- Parse Device Type
    , CASE
      WHEN {{ sanitized_user_agent }} LIKE '%iphone%' 
		    OR {{ sanitized_user_agent }} LIKE '%ios%' 
		    OR {{ sanitized_user_agent }} LIKE '%mobile%'
		    OR {{ sanitized_user_agent }} LIKE '%bingsapphire%'
        OR {{ sanitized_user_agent }} LIKE '%sm-g%' -- samsung galaxy series
		    THEN 'mobile'
      WHEN {{ sanitized_user_agent }} LIKE '%windows%' 
	  	  OR {{ sanitized_user_agent }} LIKE '%macintosh%'
		    OR {{ sanitized_user_agent }} LIKE '%mac%'
		    OR {{ sanitized_user_agent }} LIKE '%desktop%'
        OR {{ sanitized_user_agent }} LIKE '%chrome%'
	  	  THEN 'computer/desktop'
      WHEN {{ sanitized_user_agent }} LIKE '%sm-t%' -- samsung tab series
        OR {{ sanitized_user_agent }} LIKE '%pixel c%'
        OR {{ sanitized_user_agent }} LIKE '%nexus 7%'
        OR {{ sanitized_user_agent }} LIKE '%tablet%'
        OR {{ sanitized_user_agent }} LIKE '%ipad%'
        THEN 'tablet'
    END AS context_device_type

    -- Browser Category
    , CASE 
        WHEN {{ sanitized_user_agent }} LIKE '%iphone%' 
		      OR {{ sanitized_user_agent }} LIKE '%ios%' 
		      OR {{ sanitized_user_agent }} LIKE '%mobile%'
		      OR {{ sanitized_user_agent }} LIKE '%bingsapphire%'
          OR {{ sanitized_user_agent }} LIKE '%sm-g%' -- samsung galaxy series
          OR {{ sanitized_user_agent }} LIKE '%sm-t%' -- samsung tab series
          OR {{ sanitized_user_agent }} LIKE '%pixel c%'
          OR {{ sanitized_user_agent }} LIKE '%nexus 7%'
          OR {{ sanitized_user_agent }} LIKE '%tablet%'
          OR {{ sanitized_user_agent }} LIKE '%ipad%'
          THEN 'mobile'
        WHEN {{ sanitized_user_agent }} LIKE '%spider%' 
          OR {{ sanitized_user_agent }} LIKE '%bot%' 
          OR {{ sanitized_user_agent }} LIKE '%crawler%' 
          OR {{ sanitized_user_agent }} LIKE '%google-read-aloud%' 
          OR {{ sanitized_user_agent }} LIKE '%facebookexternalhit%' 
          OR {{ sanitized_user_agent }} LIKE '%facebookcatalog%' 
          THEN 'crawler'
        WHEN {{ sanitized_user_agent }} LIKE '%tv%' 
          OR {{ sanitized_user_agent }} LIKE '%appliance%'
          OR {{ sanitized_user_agent }} LIKE '%playstation%'
          OR {{ sanitized_user_agent }} LIKE '%xbox%'
          OR {{ sanitized_user_agent }} LIKE '%nintendo%'
          THEN 'appliance'
        WHEN {{ sanitized_user_agent }} LIKE '%windows%' 
          OR {{ sanitized_user_agent }} LIKE '%macintosh%'
          OR {{ sanitized_user_agent }} LIKE '%mac%'
          OR {{ sanitized_user_agent }} LIKE '%desktop%'
          OR {{ sanitized_user_agent }} LIKE '%chrome%' 
          THEN 'desktop'
        ELSE 'unknown'
    END AS browser_category    

    -- Browser Name
    , CASE
        WHEN {{ sanitized_user_agent }} LIKE '%crios%'
          OR {{ sanitized_user_agent }} LIKE '%chrome%' THEN 'chrome'
        WHEN {{ sanitized_user_agent }} LIKE '%safari%' AND {{ sanitized_user_agent }} NOT LIKE '%chrome%' 
          THEN 'safari'
        WHEN {{ sanitized_user_agent }} LIKE '%firefox%' 
          THEN 'firefox'
        WHEN {{ sanitized_user_agent }} LIKE '%edge%' THEN 'edge'
        WHEN {{ sanitized_user_agent }} LIKE '%msie%' 
          OR {{ sanitized_user_agent }} LIKE '%trident%' 
          THEN 'internet explorer'
        WHEN {{ sanitized_user_agent }} LIKE '%opera%' 
          OR {{ sanitized_user_agent }} LIKE '%opr%' 
          THEN 'opera'
        WHEN {{ sanitized_user_agent }} LIKE '%googlebot%' 
          THEN 'googlebot'
        ELSE 'unknown'
    END AS browser_name

    -- Browser Vendor
    , CASE
        WHEN {{ sanitized_user_agent }} LIKE '%applewebkit%' 
          THEN 'apple'
        WHEN {{ sanitized_user_agent }} LIKE '%googlebot%' 
          OR {{ sanitized_user_agent }} LIKE '%chrome%' 
          THEN 'google'
        WHEN {{ sanitized_user_agent }} LIKE '%edge%' 
          OR {{ sanitized_user_agent }} LIKE '%msie%' 
          OR {{ sanitized_user_agent }} LIKE '%trident%' 
          THEN 'microsoft'
        WHEN {{ sanitized_user_agent }} LIKE '%opera%' 
          OR {{ sanitized_user_agent }} LIKE '%opr%' 
          THEN 'opera'
        ELSE 'unknown'
    END AS browser_vendor
{% endmacro %}
----------------------------------------------------------------------------------------------------
{% macro create_legacy_sessions() %}
  -- get time frames
  {% set time_query %}
    SELECT
      date
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2025-03-01', INTERVAL 1 DAY)) AS date
    ORDER BY 1
  {% endset %}

  {% set time_result = dbt_utils.get_query_results_as_dict(time_query) %}
  
  {% set time_frames = ["< '2020-01-01'"] %}
  {% for row in time_result['frames'] %}
    {% do time_frames.append(row) %}
  {% endfor %}


  -- create session flags
  {% do log("Creating session flags", info=True) %}
  {% set legacy_session_flags = this.database ~ '.' ~ this.schema ~ '.base_customer_data_platform__legacy_session_flags' %}
  {% set create_legacy_session_flags %}
        CREATE OR REPLACE TABLE {{ legacy_session_flags }} AS
        
        SELECT
            *,
            /*
              First event
            */
            IF(event_sequence = 1, 'first', NULL) AS first_session,
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
                THEN 'timebox'
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
                THEN 'campaign-based'
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
                THEN 'system-based'
            END AS new_platform_based_session
        FROM {{ ref("base_customer_data_platform__legacy_events") }}
  {% endset %}

  {% do run_query(create_legacy_session_flags) %}
  {% do log("Session flags created.", info=True) %}
  

  -- create session relations
  {% for frame in time_frames %}
    {% set relation_name = this.database ~ '.' ~ this.schema ~ '.base_customer_data_platform__legacy_sessions_' ~ loop.index %}
    {% do log("Creating session relation: " ~ relation_name, info=True) %}
    {% set query %}
      WITH create_sessions AS (
          SELECT
            *
            , CONCAT(
              source_name, '_', event_id, '_',
              COALESCE(first_session, new_time_based_session, new_campaign_based_session, new_platform_based_session)
            ) AS session_id
          FROM `{{ legacy_session_flags }}`
          WHERE source_name = '{{ cdp_source }}'
            AND event_at {{ frame }}
            AND (first_session IS NOT NULL
            OR new_time_based_session IS NOT NULL
            OR new_campaign_based_session IS NOT NULL
            OR new_platform_based_session IS NOT NULL)
      )
      SELECT
        curr.*
        , previous.session_id
      FROM `{{ legacy_session_flags }}` AS curr
      LEFT JOIN create_sessions AS previous
        ON curr.source_name = previous.source_name
        AND curr.event_at BETW
      WHERE curr.source_name = '{{ cdp_source }}'
        AND curr.event_at {{ frame }}
        AND (curr.first_session IS NULL
          AND curr.new_time_based_session IS NULL
          AND curr.new_campaign_based_session IS NULL
          AND curr.new_platform_based_session IS NULL)
    {% endset %}

    {% do run_query(query) %}
  {% endfor %}
  
  {% do log("Finished creating session relations", info=True) %}

  {% do return("SELECT 'success'") %}
{% endmacro %}


----------------------------------------------------------------------------------------------------
{% macro union_legacy_sessions() %}
  -- get relations
  {%- set session_events = dbt_utils.get_relations_by_pattern(
      schema_pattern = this.schema,
      table_pattern = 'base_customer_data_platform__legacy_sessions_%',
      database = this.database
  ) -%}

    {% for events in session_events %}
        
        SELECT * FROM {{ events }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
{% endmacro %}
