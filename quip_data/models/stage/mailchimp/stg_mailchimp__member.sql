WITH source AS (
    SELECT * FROM {{ source('mailchimp', 'member') }}
)

SELECT
    _fivetran_synced AS source_synced_at
    , * EXCEPT(_fivetran_synced)
FROM source