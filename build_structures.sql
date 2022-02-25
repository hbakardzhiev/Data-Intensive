CREATE INDEX price ON "Inventory" ("InventoryPrice");

CREATE MATERIALIZED VIEW "PurchasePrice" (InventoryPrice, ProductId, CustomerId) AS
SELECT "InventoryPrice", "Purchase"."ProductId", "Purchase"."CustomerId"
FROM "Purchase",
     "Inventory"
WHERE "Purchase"."ProductId" = "Inventory"."ProductId"
  AND "Purchase"."StoreId" = "Inventory"."StoreId"
  AND "Purchase"."PurchaseDate" = "Inventory"."InventoryDate"
ORDER BY "InventoryPrice";