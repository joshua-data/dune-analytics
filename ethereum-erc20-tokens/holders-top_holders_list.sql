WITH
CTE_summary AS (
    SELECT
        *,
        balance_token / SUM(balance_token) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS share
    FROM
        query_3104364 -- holders
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    balance_token DESC
LIMIT
    10
;