CREATE UNIQUE INDEX year_index ON "Customer" ("CustomerMaritalStatus", extract(year FROM "CustomerDateOfBirth"),
                                              "CustomerId");

CREATE INDEX postal_year ON "Customer" ("CustomerPostcode", "CustomerDateOfBirth");

EXPLAIN ANALYSE CREATE MATERIALIZED VIEW "PurchasePrice" (InventoryPrice, ProductId, CustomerId) AS
SELECT "InventoryPrice", "Purchase"."ProductId", "Purchase"."CustomerId"
FROM "Purchase",
     "Inventory"
WHERE "Purchase"."ProductId" = "Inventory"."ProductId"
  AND "Purchase"."StoreId" = "Inventory"."StoreId"
  AND "Purchase"."PurchaseDate" = "Inventory"."InventoryDate";

CREATE INDEX inventory_price ON "PurchasePrice" ("inventoryprice" desc, productid);

CREATE MATERIALIZED VIEW "CountView" AS
SELECT "CustomerPostcode", "CustomerDateOfBirth", count("Purchase") as "NumberOfPurchases"
FROM "Purchase",
     "Customer"
WHERE "Customer"."CustomerId" = "Purchase"."CustomerId"
GROUP BY "CustomerPostcode", "CustomerDateOfBirth"
HAVING count("Purchase") >= 1000
ORDER BY "NumberOfPurchases";

CREATE INDEX purchase_product_date ON "Purchase" USING hash ("PurchaseDate");

CREATE INDEX price ON "Inventory" ("InventoryPrice");