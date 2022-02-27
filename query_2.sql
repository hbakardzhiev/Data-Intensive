SELECT DISTINCT "Inventory"."ProductId"
FROM "Inventory"
WHERE "InventoryPrice" >= %(p)s
EXCEPT
(
    SELECT DISTINCT "PurchasePrice"."productid"
    FROM "PurchasePrice"
    WHERE "PurchasePrice"."inventoryprice" >= %(p)s
)
ORDER BY "ProductId";