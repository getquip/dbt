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
{% macro create_touchpoint(context_page_path) %}
	{% set query %}
		REGEXP_REPLACE(
			{{ context_page_path}}
        	, r'([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})|(sub_[a-z0-9]+)|([0-9]{6,})'
				, "<< removed >>"
		) AS touchpoint
	{% endset %}

	{% do return(query) %}
{% endmacro %}
