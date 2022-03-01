SELECT "Purchase"."ProductId", "Purchase"."CustomerId", "Purchase"."StoreId", "Purchase"."PurchaseDate",
"Inventory"."InventoryPrice", "Customer"."CustomerName", "Customer"."CustomerAddress",
"Customer"."CustomerPostcode", "Customer"."CustomerMaritalStatus",
"Customer"."CustomerDateOfBirth", "Store"."StoreName", "Store"."StoreAddress",
"Store"."StorePostcode", "Product"."ProductName",
"Product"."ProductCategory", "Product"."ProductDescription"
FROM "Purchase", "Inventory", "Customer", "Store", "Product"
WHERE "Purchase"."CustomerId" = "Customer"."CustomerId" AND "Purchase"."CustomerId" = 2
AND "Purchase"."StoreId" = "Store"."StoreId" AND "Purchase"."StoreId" = "Inventory"."StoreId" AND "Purchase"."StoreId" = 117
AND "Purchase"."ProductId" = "Product"."ProductId" AND "Purchase"."ProductId" = "Inventory"."ProductId" AND "Purchase"."ProductId" = 1208904 
AND "Purchase"."PurchaseDate" =  '07-04-2021';