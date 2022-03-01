WITH ProdCat AS (SELECT "ProductId", "ProductName"
                 FROM "Product"
                 WHERE "ProductCategory" = %(c)s)
SELECT ProdCat."ProductId", "ProductName"
FROM ProdCat,
     "Purchase"
WHERE EXTRACT(DAY FROM "PurchaseDate") = %(d)s
  AND EXTRACT(MONTH FROM "PurchaseDate") = %(m)s
ORDER BY "ProductId";