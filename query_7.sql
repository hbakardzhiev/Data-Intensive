with productID_Date_Quantity (productID, date, count) AS
         (select distinct pur."ProductId", pur."PurchaseDate", count(*)
          from "Purchase" as pur,
               "Product" as p
          where pur."ProductId" = p."ProductId"
            and p."ProductCategory" = %(c)s
            and pur."PurchaseDate" BETWEEN %(s)s and %(e)s
          group by(pur."ProductId", pur."PurchaseDate"))
select productID, var_pop(count) as PopulationVariance
from productID_Date_Quantity
group by productID
having var_pop(count) > 5;