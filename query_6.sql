WITH ProductView AS (
    SELECT "ProductId", "ProductName", "ProductCategory"
    FROM "Product"
    WHERE "ProductCategory" = %(c)s),

     PurchaseView1 AS (
         SELECT pur."ProductId", pur."PurchaseDate"
         FROM "Purchase" as pur
         WHERE EXTRACT(DAY FROM "PurchaseDate") = %(d)s
           AND EXTRACT(MONTH FROM "PurchaseDate") = %(m)s)
SELECT ProductView."ProductId", "ProductName", "ProductCategory", "PurchaseDate"
FROM ProductView,
     PurchaseView1
WHERE ProductView."ProductId" = PurchaseView1."ProductId"