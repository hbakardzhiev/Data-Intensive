EXPLAIN ANALYSE
SELECT "CustomerPostcode", "CustomerDateOfBirth", count("Purchase")
FROM "Purchase",
     "Customer"
WHERE "Customer"."CustomerId"="Purchase"."CustomerId"
GROUP BY "CustomerPostcode", "CustomerDateOfBirth"