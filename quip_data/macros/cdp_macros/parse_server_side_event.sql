{% macro parse_server_side_event(context_library_name) %}

  CASE
    WHEN {{ context_library_name }} IN ('analytics-ruby', '@segment/analytics-node', 'RudderStack Shopify Cloud')
      THEN 'server-side'
    WHEN {{ context_library_name }} IN ('analytics-ios', 'analytics.js', 'analytics-android', 'analytics-kotlin', 'RudderLabs JavaScript SDK')
      THEN 'client-side'
    ELSE 'unknown'
  END AS call_type
	
{% endmacro %}