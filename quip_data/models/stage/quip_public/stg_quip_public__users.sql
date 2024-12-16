WITH source AS (
    SELECT * FROM {{ source("quip_public", "users") }}
)

, renamed AS (
    SELECT
        -- ids
        id AS legacy_quip_user_id
        , cart_id
        , invited_by_id	AS invited_by_quip_user_id
        , external_id

        -- timestamps
        , COALESCE(_fivetran_deleted, FALSE) AS is_source_deleted
        , _fivetran_synced AS source_synced_at
        , SAFE_CAST(created_at AS TIMESTAMP) AS created_at
        , SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
        , confirmation_sent_at
        , confirmed_at
        , current_sign_in_at
        , invitation_created_at
        , invitation_accepted_at		
        , invitation_sent_at
        , last_sign_in_at
        , remember_created_at	


         , {{ generate_hashed_pii_fields([
            'email'
            , 'phone'
            ]) }}
        , LOWER(city) AS city
        , LOWER(country) AS country
        , flags
        , invited_by_type -- is null
        , encrypted_password
        , confirmation_token		
        , current_sign_in_ip			
        , customer_facing_agent_name			
        , delivery_instructions	
        , `group`
        , landing_url						
        , last_sign_in_ip			
        , `name` AS first_name
        , `name` AS last_name
        , postal_code
        , referring_url					
        , reset_password_sent_at			
        , reset_password_token			
        , LOWER(role) AS role			
        , shipping_name	
        , `state`		
        , street_address			
        , street_address_unit			
        , stripe_customer_id			
        , unconfirmed_email	-- is null
        , feature_flags	
        , LOWER(user_type) AS user_type
        , invitation_token

        -- bools
        , is_verified
        , is_internally_verified
        , is_active
        , signed_up_on_shopify AS is_shopify_user
        , smart_brush_user	AS is_smart_brush_user
        , should_receive_emails	AS is_subscribed_to_emails
        , needs_to_readd_a_payment_method
        , milestones	
        , registration_origin	

        -- ints
        , free_head_counter AS free_head_count
        , quip_credit
        , sign_in_count		
        , referral_count
        , verification_status	-- what is this?
        , invitation_limit	
        , invitations_count
    FROM source
)

SELECT * FROM renamed
WHERE NOT is_source_deleted