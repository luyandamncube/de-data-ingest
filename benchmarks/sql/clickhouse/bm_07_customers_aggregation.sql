SELECT
    province,
    count() AS customer_count
FROM file('/benchmarks/data/customers.csv', CSVWithNames)
GROUP BY province
ORDER BY province
FORMAT JSONEachRow
