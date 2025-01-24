{{ config(
    partition_by={
      "field": "source_synced_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
	cluster_by=[
        "address_type",
        "phone",
        "postal_code_last_four", 
        "legacy_customer_id"
    ]
) }}

WITH source AS (
    SELECT * FROM {{ source('quip_public', 'addresses') }}
)

, renamed AS (
    SELECT
        -- ids
        id AS legacy_customer_id
        , addressable_id --?
        -- timestamps
        , _fivetran_synced AS source_synced_at
        , created_at
        , updated_at
        , address_type

        , addressable_type
        , care_of
        , name
        , phone
        , postal_code
        , zipcode_last_four AS postal_code_last_four
        , state
        , street_address
        , street_address_unit
        , is_verified_by_easypost
        , COALESCE(_fivetran_deleted , FALSE) AS is_source_deleted
        , LOWER(city) AS city

        -- boolean
        , LOWER(country) AS country
    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted
