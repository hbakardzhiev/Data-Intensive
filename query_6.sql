WITH ProductView AS (
    SELECT "ProductId", "ProductName", "ProductCategory"
    FROM "Product"
    WHERE "ProductCategory" = 'ZA2'),

    PurchaseView1 AS(
    SELECT pur."ProductId", pur."PurchaseDate"
    FROM "Purchase" as pur
    where pur."PurchaseDate" LIKE '26-8%')
--     WHERE EXTRACT(DAY FROM "PurchaseDate") = '10' AND
--           EXTRACT(MONTH FROM "PurchaseDate") = '5')
SELECT ProductView."ProductId", "ProductName" --, "ProductCategory", "PurchaseDate"
FROM ProductView, PurchaseView1
WHERE ProductView."ProductId" = PurchaseView1."ProductId"