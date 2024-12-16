-- WITH source AS (
-- 	SELECT * FROM {{ source('recharge', 'events') }}
-- )

-- SELECT 
-- 	id
-- 	, customer_id
-- 	, object_id
-- 	, created_at
-- 	, `custom_attributes`[SAFE_OFFSET(0)].`key`
-- 	, `custom_attributes`[SAFE_OFFSET(0)].value
-- 	, description, object_type
-- 	, `source`.account_id
-- 	, `source`.api_token_id
-- 	, `source`.account_email
-- 	, `source`.api_token_name
-- 	, `source`.origin
-- 	, `source`.user_type
-- 	, `updated_attributes`[SAFE_OFFSET(0)].attribute
-- 	, `updated_attributes`[SAFE_OFFSET(0)].previous_value
-- 	, `updated_attributes`[SAFE_OFFSET(0)].value
-- 	, verb 
-- FROM source

SELECT "pass" AS pass