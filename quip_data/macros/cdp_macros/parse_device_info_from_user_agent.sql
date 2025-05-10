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