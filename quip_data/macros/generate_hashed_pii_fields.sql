-- This macro takes a list of field names and returns a list of field names and the hashed version of the field name appended with "_hashed".
-- Example:
-- {{ generate_hashed_pii_fields(['email', 'phone']) }}
-- Returns:
    -- email
    -- , TO_HEX(SHA256(email)) AS email_hashed
    -- , phone
    -- , TO_HEX(SHA256(phone)) AS phone_hashed
{% macro generate_hashed_pii_fields(field_names) %}

    {%- set result = [] -%}
    {%- for field in field_names -%}
        {{ result.append(field) }}
        {{ result.append("TO_HEX(SHA256(" ~ field ~ ")) AS " ~ field ~ "_hashed") }}
    {%- endfor -%}
    {{ return(result | join(', ')) }}
{% endmacro %}
