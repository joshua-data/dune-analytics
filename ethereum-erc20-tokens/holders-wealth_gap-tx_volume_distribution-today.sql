WITH
-- 가장 최근 날짜의 정보만 가져온다.
CTE_only_today AS (
    SELECT
        date,
        top10,
        tx_volume_token
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY top10
                ORDER BY date DESC
            ) AS row_num
        FROM
            query_3092510 -- holders-wealth_gap-tx_volume_distribution
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