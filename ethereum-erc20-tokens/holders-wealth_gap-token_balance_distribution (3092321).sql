WITH
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY balance_token DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS rank
    FROM
        query_3101913 -- balance
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
),
-- TOP 10 address만 식별하고, 나머지는 모두 기타 처리로 묶어준다.
CTE_running_balance_top10 AS (
    SELECT
        date,
        CASE
            WHEN rank <= 10 THEN 'Top' || LPAD(CAST(rank AS VARCHAR), 2, '0')
            ELSE '(Others)'
        END AS top10,
        SUM(balance_token) AS balance_token
    FROM
        CTE_without_invalid_addresses
    GROUP BY
        1, 2
)
SELECT
    *
FROM
    CTE_running_balance_top10
ORDER BY
    date,
    top10
;