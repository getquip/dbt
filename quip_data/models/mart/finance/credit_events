SELECT
    e.*
    , f.days_between_credit_and_debit
FROM {{ ref('int_fct_payments__credit_events') }} e
LEFT JOIN {{ ref('credit_account_dims') }} f
    ON e.credit_account_id = f.credit_account_id