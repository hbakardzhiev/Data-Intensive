SELECT DISTINCT "Product"."ProductId"
FROM "Product"
EXCEPT
(
    SELECT DISTINCT "PurchasePrice"."productid"
    FROM "PurchasePrice"
    WHERE "PurchasePrice"."inventoryprice" >= %(p)s
)
ORDER BY "ProductId";