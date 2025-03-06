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

SELECT
	skus.sku
    , COALESCE(skus.weight, 0) AS weight_lb
    , hts.tariff_number
    , hts.china_tariff_number
    , skus.unit_cost
FROM skus
LEFT JOIN hts 
    ON hts.sku = skus.sku
