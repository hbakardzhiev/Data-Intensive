SELECT DISTINCT "Product"."ProductId", "Product"."ProductName"
FROM "Purchase",
     "Product"
WHERE "Purchase"."PurchaseDate" = %(d)s
  AND "Purchase"."ProductId" = "Product"."ProductId"
ORDER BY "ProductId";