CREATE INDEX price ON "Inventory" ("InventoryPrice");

CREATE INDEX year_index ON "Customer" ("CustomerMaritalStatus", extract(year FROM "CustomerDateOfBirth"));

CREATE INDEX postal_year ON "Customer" ("CustomerPostcode", "CustomerDateOfBirth");

CREATE MATERIALIZED VIEW "PurchasePrice" (InventoryPrice, ProductId, CustomerId) AS
SELECT "InventoryPrice", "Purchase"."ProductId", "Purchase"."CustomerId"
FROM "Purchase",
     "Inventory"
WHERE "Purchase"."ProductId" = "Inventory"."ProductId"
  AND "Purchase"."StoreId" = "Inventory"."StoreId"
  AND "Purchase"."PurchaseDate" = "Inventory"."InventoryDate"
ORDER BY "InventoryPrice";

CREATE MATERIALIZED VIEW "PurchaseProductDate" (PurchaseDate, ProductId, ProductName) AS
SELECT "Purchase"."PurchaseDate", "Product"."ProductId", "Product"."ProductName"
FROM "Purchase", "Product"
WHERE "Purchase"."ProductId" = "Product"."ProductId"
ORDER BY "Purchase"."PurchaseDate";

CREATE MATERIALIZED VIEW "PurchasePriceSum" AS
Select sum(totalSum) from (SELECT sum(inv."InventoryPrice") / count(pur."ProductId") as totalSum
                           from "Inventory" as inv, "Product" as pro, "Purchase" as pur
                           where inv."ProductId" = pur."ProductId" and inv."InventoryDate" = pur."PurchaseDate" and pro."ProductId" = inv."ProductId"
-- and sto."StoreId" = inv."StoreId"
                          ) totalSum;

CREATE MATERIALIZED VIEW "ProductCategoryView" AS
Select pro."ProductCategory" ,avg(inv."InventoryPrice") as avgCategory
from "Product" as pro, "Inventory" as inv, "Purchase" as pur
where pro."ProductId" = inv."ProductId" and inv."InventoryDate" = pur."PurchaseDate" and pur."ProductId" = pro."ProductId"
-- and sto."StoreId" = inv."StoreId"
group by "ProductCategory"