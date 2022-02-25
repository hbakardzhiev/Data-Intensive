SELECT "CustomerMaritalStatus",
       extract(year from "CustomerDateOfBirth")  AS "CustomerYearOfBirth",
       "ProductCategory",
       sum("PurchasePrice".inventoryprice) / count("PurchasePrice") AS "AverageSpending"
FROM "Customer",
     "PurchasePrice",
     "Product"
WHERE "PurchasePrice"."productid" = "Product"."ProductId"
  AND "PurchasePrice".productid = "Customer"."CustomerId"
GROUP BY ("CustomerMaritalStatus", extract(year FROM "CustomerDateOfBirth"), "ProductCategory");