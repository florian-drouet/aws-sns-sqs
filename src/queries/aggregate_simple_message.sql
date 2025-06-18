SELECT
    DATE_TRUNC('MINUTE', m.created_at) AS minute,
    COUNT(*)
FROM
    schema_name.table_name AS m
GROUP BY
    1
ORDER BY
    1 ASC