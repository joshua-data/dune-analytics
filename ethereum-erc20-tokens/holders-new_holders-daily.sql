WITH
-- 각 address가 일자 별로 balance > 0 이었던 적이 있었는지 조사한다.
CTE_running_balance_label AS (
    SELECT
        date,
        address,
        SUM(CASE WHEN balance_token > 0 THEN 1 ELSE 0 END) AS positive_balance_days,
        LAG(
            SUM(CASE WHEN balance_token > 0 THEN 1 ELSE 0 END),
            1,
            0
        ) OVER (
            PARTITION BY address
            ORDER BY date
        ) AS prev_positive_balance_days
    FROM
        query_3101913 -- balance
    GROUP BY
        date,
        address
),
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        CTE_running_balance_label
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
),
CTE_dnu AS (
    SELECT
        date,
        COUNT(address) AS dnu
    FROM
        CTE_without_invalid_addresses
    WHERE
        prev_positive_balance_days = 0
        AND positive_balance_days > 0
    GROUP BY
        date
)
SELECT
    *
FROM
    CTE_dnu
ORDER BY
    date
;