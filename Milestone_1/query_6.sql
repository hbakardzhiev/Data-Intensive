select distinct pro."ProductId", pro."ProductName"
from "Product" as pro, "Purchase" as pur
where pro."ProductCategory" = %(c)s and
      pro."ProductId" = pur."ProductId" and
      EXTRACT(DAY FROM pur."PurchaseDate") = %(d)s AND
      EXTRACT(MONTH FROM pur."PurchaseDate") = %(m)s
order by  pro."ProductId";         