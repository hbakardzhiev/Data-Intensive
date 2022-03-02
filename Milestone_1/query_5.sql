Select DISTINCT ProductCategory
from "TotalView", "ProductCategoryView"
where avgCategory > (select * from "TotalView") * %(f)s
order by productcategory;