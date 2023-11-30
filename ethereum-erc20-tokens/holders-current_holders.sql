WITH
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        query_3101913 -- balance
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
        AND balance_token > 0
),
-- 일자 별로 홀더 수를 집계한다.
CTE_number_of_holders AS (
    SELECT
        date,
        COUNT(address) AS holders_cnt
    FROM
        CTE_without_invalid_addresses
    GROUP BY
        date

)
SELECT
    *
FROM
    CTE_number_of_holders
ORDER BY
    date
;