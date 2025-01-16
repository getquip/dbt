WITH source AS (
    SELECT * FROM {{ source('mailchimp', 'member') }}
)

SELECT
    * EXCEPT (_fivetran_synced)
    , _fivetran_synced AS source_synced_at
FROM source
