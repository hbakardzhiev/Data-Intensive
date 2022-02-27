CREATE TYPE enum_marriage AS enum ('M', 'U');

CREATE UNLOGGED TABLE "Customer"
(
    "CustomerId"            integer not null
        constraint customer_pk
            primary key,
    "CustomerName"          varchar(70)
        constraint regexname
            check (("CustomerName")::text ~ '[ a-zA-Z0-9]+.'::text),
    "CustomerAddress"       varchar(200)
        constraint regexaddress
            check (("CustomerAddress")::text ~ '[ a-zA-Z0-9]+.'::text),
    "CustomerPostcode"      varchar(20)
        constraint regexpost
            check (("CustomerPostcode")::text ~ '[ a-zA-Z0-9]+.'::text),
    "CustomerMaritalStatus" enum_marriage,
    "CustomerDateOfBirth"   date
);

CREATE UNLOGGED TABLE "Store"
(
    "StoreId"       integer not null
        constraint store_pk
            primary key,
    "StoreName"     varchar(70)
        constraint storenameregex
            check (("StoreName")::text ~ '[ a-zA-Z0-9]+'::text),
    "StoreAddress"  varchar(200)
        constraint storeaddressregex
            check (("StoreAddress")::text ~ '[ a-zA-Z0-9]+'::text),
    "StorePostcode" varchar(20)
        constraint storepostcoderegex
            check (("StorePostcode")::text ~ '[ a-zA-Z0-9]+'::text)
);

CREATE UNIQUE INDEX store_storeid_uindex
    ON "Store" ("StoreId");

CREATE UNLOGGED TABLE "Product"
(
    "ProductId"          integer not null
        constraint product_pk
            primary key,
    "ProductName"        varchar(70)
        constraint productnameregex
            check (("ProductName")::text ~ '[ a-zA-Z0-9]+'::text),
    "ProductCategory"    varchar(70)
        constraint productcategoryregex
            check (("ProductCategory")::text ~ '[ a-zA-Z0-9]+'::text),
    "ProductDescription" varchar(300)
        constraint productdescriptionregex
            check (("ProductDescription")::text ~ '[ \.a-zA-Z0-9]+'::text)
);

CREATE UNLOGGED TABLE "Inventory"
(
    "StoreId"        integer,
    "InventoryDate"  date,
    "ProductId"      integer,
    "InventoryPrice" numeric(7, 2)
);

CREATE UNLOGGED TABLE "Purchase"
(
    "ProductId"    integer,
    "CustomerId"   integer,
    "StoreId"      integer,
    "PurchaseDate" date
);