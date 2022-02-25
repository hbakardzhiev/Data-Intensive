-- SELECT DISTINCT "Product"."ProductId"
EXPLAIN ANALYSE SELECT DISTINCT "Product"."ProductId"
FROM "Product"

EXCEPT(
    SELECT DISTINCT "PurchasePrice"."productid"
    FROM "PurchasePrice"
    -- WHERE "PurchasePrice"."InventoryPrice" >= %(p)s
    WHERE "PurchasePrice"."inventoryprice" >= 5000.00
)

ORDER BY "ProductId";