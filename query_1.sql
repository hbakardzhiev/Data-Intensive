-- SELECT DISTINCT pr.ProductId, pr.ProductName
EXPLAIN ANALYSE SELECT DISTINCT "Product"."ProductId", "Product"."ProductName"
FROM "Product", "Purchase"
-- WHERE pr.ProductId = pu.ProductId AND pu.PurchaseDate = %(d)s
WHERE "Product"."ProductId" = "Purchase"."ProductId" AND "Purchase"."PurchaseDate" = '26-08-2019'
ORDER BY "Product"."ProductId";