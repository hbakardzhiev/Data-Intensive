-- SELECT DISTINCT pr.ProductId, pr.ProductName
EXPLAIN ANALYSE SELECT DISTINCT "PurchaseProductDate"."productid", "PurchaseProductDate"."productname"
FROM "PurchaseProductDate"
-- WHERE "PurchaseDate" = %(d)s
WHERE "PurchaseProductDate"."purchasedate" = '26-08-2019'
ORDER BY "PurchaseProductDate"."productid";