WITH

users AS (
	SELECT * FROM {{ ref('int_dim_users') }}
)
-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

SELECT
	users.*
FROM users