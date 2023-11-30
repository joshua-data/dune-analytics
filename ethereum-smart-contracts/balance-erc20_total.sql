WITH
CTE_summary AS (
    SELECT
        date,
        SUM(balance_usd) AS balance_usd
    FROM
        query_3109864 -- balance_erc20
    GROUP BY
        date
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    date
;