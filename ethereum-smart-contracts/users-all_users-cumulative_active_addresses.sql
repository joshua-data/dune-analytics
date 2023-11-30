WITH
-- 해당 컨트랙트를 건드리는 traces를 모두 가져오기
CTE_traces AS (
    SELECT
        DATE_TRUNC('DAY', block_time) AS date,
        "from",
        tx_hash
    FROM
        ethereum.traces
    WHERE
        to = 0x5283D291DBCF85356A21bA090E6db59121208b44
        AND success = True
),
-- Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Array 가져오기
CTE_traces_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(date) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM CTE_traces
    ) AS DATE_LIMITS
    CROSS JOIN
        UNNEST(
            SEQUENCE(
                DATE_LIMITS.min_date,
                DATE_LIMITS.max_date,
                INTERVAL '1' DAY
            )
        ) AS T(DATE_COLUMN)
),
-- Running한 DAU도 함께 가져온다.
CTE_dau AS (
    SELECT
        FRAME.date,
        COUNT(DISTINCT MAIN."from") AS dau,
        (
            SELECT COUNT(DISTINCT DAU_RUNNING."from")
            FROM CTE_traces DAU_RUNNING
            WHERE DAU_RUNNING.date <= FRAME.date
        ) AS cumulative_dau
    FROM
        CTE_traces_dates_frame FRAME
    LEFT JOIN
        CTE_traces MAIN
        ON FRAME.date = DATE_TRUNC('DAY', MAIN.date)
    GROUP BY
        1
)
SELECT
    *
FROM
    CTE_dau
ORDER BY
    date
;