CREATE UNIQUE INDEX year_index ON "Customer" ("CustomerMaritalStatus", extract(year FROM "CustomerDateOfBirth"),
                                              "CustomerId");

CREATE INDEX postal_year ON "Customer" ("CustomerPostcode", "CustomerDateOfBirth");

CREATE MATERIALIZED VIEW "PurchasePrice" (InventoryPrice, ProductId, CustomerId, ProductCategory) AS
SELECT "InventoryPrice", "Purchase"."ProductId", "Purchase"."CustomerId", "ProductCategory"
FROM "Purchase",
     "Inventory", "Product"
WHERE "Purchase"."ProductId" = "Inventory"."ProductId"
  AND "Purchase"."StoreId" = "Inventory"."StoreId"
  AND "Purchase"."PurchaseDate" = "Inventory"."InventoryDate"
  AND "Product"."ProductId" = "Inventory"."ProductId";

CREATE INDEX inventory_price ON "PurchasePrice" ("inventoryprice" desc, productid);

CREATE MATERIALIZED VIEW "CountView" AS
SELECT "CustomerPostcode", "CustomerDateOfBirth", count("Purchase") as "NumberOfPurchases"
FROM "Purchase",
     "Customer"
WHERE "Customer"."CustomerId" = "Purchase"."CustomerId"
GROUP BY "CustomerPostcode", "CustomerDateOfBirth"
HAVING count("Purchase") >= 1000
ORDER BY "NumberOfPurchases";

CREATE INDEX pro_pur ON "Purchase"( "ProductId", "PurchaseDate");

CREATE INDEX price ON "Inventory" ("InventoryPrice");

CREATE INDEX cat ON "Product" ("ProductCategory");

CREATE MATERIALIZED VIEW "TotalView" AS
Select sum(totalSum) from (SELECT sum(InventoryPrice) / count(ProductId) as totalSum
from "PurchasePrice") as PPtS;

CREATE MATERIALIZED VIEW "ProductCategoryView" AS
Select ProductCategory, avg(InventoryPrice) as avgCategory
from "PurchasePrice"
group by ProductCategory;

CREATE INDEX purchase_date_extracts ON "Purchase" (extract(day from "PurchaseDate"),
                                                     extract(month from "PurchaseDate"));