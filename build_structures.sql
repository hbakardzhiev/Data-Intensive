CREATE INDEX price ON "Inventory" ("InventoryPrice");

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