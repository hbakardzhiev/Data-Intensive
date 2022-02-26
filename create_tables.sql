create type enum_marriage as enum ('M', 'U');

create table if not exists "Customer"
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

create table if not exists "Store"
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

create unique index if not exists store_storeid_uindex
    on "Store" ("StoreId");

create table if not exists "Product"
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

create table if not exists "Inventory"
(
    "StoreId"        integer,
    "InventoryDate"  date,
    "ProductId"      integer,
    "InventoryPrice" numeric(7, 2)
);

create table if not exists "Purchase"
(
    "ProductId"    integer,
    "CustomerId"   integer,
    "StoreId"      integer,
    "PurchaseDate" date
);