WITH
-- Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Array 가져오기
CTE_transfer_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(date) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM query_3099260 -- transfer
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
-- WAU와 MAU는 Running한 값으로 부여한다.
CTE_dau_wau_mau AS (
    SELECT
        FRAME.date,
        COUNT(DISTINCT MAIN.from_address) AS dau_from,
        COUNT(DISTINCT MAIN.to_address) AS dau_to,
        (
            SELECT COUNT(DISTINCT WAU_FROM.from_address)
            FROM query_3099260 WAU_FROM -- transfer
            WHERE DATE_ADD('DAY', -6, FRAME.date) <= WAU_FROM.date AND WAU_FROM.date <= FRAME.date
        ) AS wau_from,
        (
            SELECT COUNT(DISTINCT WAU_TO.to_address)
            FROM query_3099260 WAU_TO -- transfer
            WHERE DATE_ADD('DAY', -6, FRAME.date) <= WAU_TO.date AND WAU_TO.date <= FRAME.date
        ) AS wau_to,
        (
            SELECT COUNT(DISTINCT MAU_FROM.from_address)
            FROM query_3099260 MAU_FROM -- transfer
            WHERE DATE_ADD('DAY', -29, FRAME.date) <= MAU_FROM.date AND MAU_FROM.date <= FRAME.date
        ) AS mau_from,
        (
            SELECT COUNT(DISTINCT MAU_TO.to_address)
            FROM query_3099260 MAU_TO -- transfer
            WHERE DATE_ADD('DAY', -29, FRAME.date) <= MAU_TO.date AND MAU_TO.date <= FRAME.date
        ) AS mau_to
    FROM
        CTE_transfer_dates_frame FRAME
    LEFT JOIN
        query_3099260 MAIN -- transfer
        ON FRAME.date = MAIN.date
    GROUP BY
        1
)
SELECT
    *
FROM
    CTE_dau_wau_mau
ORDER BY
    date
;