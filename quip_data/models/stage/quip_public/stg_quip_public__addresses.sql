WITH source AS (
    SELECT * FROM {{ source('quip_public', 'addresses') }}
)

, renamed AS (
    SELECT
        -- ids
        id	INTEGER	
        , addressable_id
        -- timestamps
        , _fivetran_deleted AS is_source_deleted
        , _fivetran_synced AS synced_at
        , created_at
        , updated_at

        , address_type
        , addressable_type
        , care_of
        , city
        , country
        , name
        , phone
        , postal_code
        , zipcode_last_four AS postal_code_last_four
        , state
        , street_address
        , street_address_unit

        -- boolean
        , is_verified_by_easypost
    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted
