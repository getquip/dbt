WITH

shipment_items AS (
	-- at the sku level
	SELECT * FROM {{ ref("stg_wen_parker__shipment_item_details") }}
)

, skus AS (
	SELECT * FROM {{ ref("int_dim_products__skus") }}
)

-------------------------------------------------------
----------------- FINISH REFERENCES -------------------
-------------------------------------------------------

, house_bill_item_summary AS (
	SELECT
		items.house_bill_number
		, items.sku
		, skus.sku_presentment
		, items.po_number
		, skus.tariff_number
		, skus.china_tariff_number
		-- quantity
		, items.quantity AS total_sku_quantity
		, SUM(items.quantity) OVER (PARTITION BY items.house_bill_number) AS total_hbl_item_quantity 
		-- weight
		, skus.weight_lb * items.quantity AS total_sku_weight_lb
		, SUM(skus.weight_lb * items.quantity) OVER (PARTITION BY items.house_bill_number) AS total_hbl_weight_lb
		-- tariffs
		, skus.unit_cost -- need to coalesce with cogs with this as secondary
		, skus.unit_cost * items.quantity AS total_sku_cost
	FROM shipment_items AS items
	-- this join filters out any skus from shipment_items that are not in the skus table
	INNER JOIN skus
		ON items.sku = skus.sku
)

, cost_basis AS (
	SELECT
		house_bill_number
		, SUM(skus.unit_cost * items.quantity) 
			OVER (PARTITION BY items.house_bill_number, skus.tariff_number) AS total_tariff_cost_basis
		, SUM(skus.unit_cost * items.quantity) 
			OVER (PARTITION BY items.house_bill_number, skus.china_tariff_number) AS total_china_tariff_cost_basis
	FROM house_bill_item_summary
)

SELECT
	*
	, SAFE_DIVIDE(total_sku_quantity, total_hbl_item_quantity) AS allocation_percentage_by_quantity
	, SAFE_DIVIDE(total_sku_weight_lb, total_hbl_weight_lb) AS allocation_percentage_by_weight
	, SAFE_DIVIDE(total_sku_cost, total_tariff_cost_basis) AS allocation_percentage_for_tariffs
	, SAFE_DIVIDE(total_sku_cost, total_china_tariff_cost_basis) AS allocation_percentage_for_china_tariffs
FROM house_bill_item_summary