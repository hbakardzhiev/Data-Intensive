SELECT DISTINCT "PurchaseProductDate"."productid", "PurchaseProductDate"."productname"
FROM "PurchaseProductDate"
WHERE "purchasedate"=%(d)s
ORDER BY "PurchaseProductDate"."productid";