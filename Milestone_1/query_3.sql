SELECT "CustomerMaritalStatus",
       extract(year from "CustomerDateOfBirth")                     AS "CustomerYearOfBirth",
       "ProductCategory",
       sum("PurchasePrice".inventoryprice) / count("PurchasePrice") AS "AverageSpending"
FROM "Customer",
     "PurchasePrice",
     "Product"
WHERE "PurchasePrice"."productid" = "Product"."ProductId"
  AND "PurchasePrice".customerid = "Customer"."CustomerId"
GROUP BY CUBE ("CustomerMaritalStatus", "CustomerYearOfBirth",
               "ProductCategory")
ORDER BY "CustomerMaritalStatus", "CustomerYearOfBirth",
         "ProductCategory", "AverageSpending";