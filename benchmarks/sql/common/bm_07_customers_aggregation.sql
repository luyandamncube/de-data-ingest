SELECT
    province,
    COUNT(*) AS customer_count
FROM customers
GROUP BY province
ORDER BY province
