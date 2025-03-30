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
{% macro union_legacy_segment_sources() %}
  {% set test = 1 %}
{% endmacro %}
