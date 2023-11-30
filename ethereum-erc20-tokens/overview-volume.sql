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
-- 일자별로 Volume 값 가져오기
CTE_volumes AS (
    SELECT
        FRAME.date,
        COUNT(DISTINCT TRANSFER.evt_tx_hash) AS txs_cnt,
        SUM(TRANSFER.value_token) AS volume_token
    FROM
        CTE_transfer_dates_frame FRAME
    LEFT JOIN
        query_3099260 TRANSFER -- transfer
        ON FRAME.date = TRANSFER.date
    GROUP BY
        1
),
-- USD CLose Price 정보 추가해주기
CTE_volumes_with_usd AS (
    SELECT
        VOLUMES.date,
        VOLUMES.txs_cnt,
        VOLUMES.volume_token,
        VOLUMES.volume_token * USD.close_price AS volume_usd
    FROM
        CTE_volumes VOLUMES
    LEFT JOIN
        query_3089203 USD-- price
        ON VOLUMES.date = USD.date
    GROUP BY
        1, 2, 3, 4
)
SELECT
    *
FROM
    CTE_volumes_with_usd
ORDER BY
    date
;