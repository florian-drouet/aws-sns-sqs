SELECT
    DATE_TRUNC('MINUTE', m.closed_utc_at) AS minute,
    COUNT(*)
FROM
    "public".messages AS m
GROUP BY
    1
ORDER BY
    1 ASC