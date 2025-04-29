WITH

skus AS (
    SELECT * FROM {{ ref("seed__tmp_item_master") }}
)

, hts AS (
    SELECT * FROM {{ ref("stg_wen_parker__hts_duties") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, cleaned AS(
    SELECT
    	REPLACE(REPLACE(skus.sku, '-R', ''), '-Dental', '') AS sku_presentment
        , REGEXP_REPLACE(skus.sku, r'\D', '') AS sku -- remove non-numeric characters
        , COALESCE(SAFE_CAST(skus.weight AS NUMERIC), 0) AS weight_lb
        , hts.tariff_number
        , hts.china_tariff_number
        , COALESCE(SAFE_CAST(skus.unit_cost AS NUMERIC), 0) AS unit_cost
    FROM skus
    LEFT JOIN hts 
        ON hts.sku = REGEXP_REPLACE(skus.sku, r'\D', '')
    WHERE skus.sku IS NOT NULL
)

SELECT
    * EXCEPT(sku)
    , IF(sku = '', sku_presentment, sku) AS sku
FROM cleaned
