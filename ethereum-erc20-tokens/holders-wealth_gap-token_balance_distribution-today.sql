WITH
-- 가장 최근 날짜의 정보만 가져온다.
CTE_only_today AS (
    SELECT
        date,
        top10,
        balance_token
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY top10
                ORDER BY date DESC
            ) AS row_num
        FROM
            query_3092321 -- holders-wealth_gap-token_balance_distribution
    )
    WHERE
        row_num = 1
)
SELECT
    *
FROM
    CTE_only_today
ORDER BY
    date,
    top10
;