SELECT *
FROM "CountView"
WHERE "NumberOfPurchases" >= %(n)s
ORDER BY "CustomerPostcode", "CustomerDateOfBirth";