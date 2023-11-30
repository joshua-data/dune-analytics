WITH
-- Transfer에 참여한 적 있는 모든 address 리스트 가져오기
CTE_transfer_addresses_frame AS (
    SELECT
        DISTINCT address
    FROM (
        SELECT from_address AS address
        FROM query_3099260 -- transfer
        UNION ALL
        SELECT to_address AS address
        FROM query_3099260 -- transfer
    )
),
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
CTE_transfer_frame AS (
    SELECT
        DATES.date,
        ADDRESSES.address
    FROM
        CTE_transfer_dates_frame DATES
    CROSS JOIN
        CTE_transfer_addresses_frame ADDRESSES
),
-- 일자별로 각 address의 TX Volume을 집계한다.
CTE_tx_volume AS (
    SELECT
        FRAME.date,
        FRAME.address,
        SUM(TRANSFERS.value_token) AS tx_volume_token
    FROM
        CTE_transfer_frame FRAME
    LEFT JOIN
        query_3099260 TRANSFERS -- transfer
        ON FRAME.date = TRANSFERS.date AND FRAME.address = TRANSFERS.from_address
    GROUP BY
        1, 2
),
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        CTE_tx_volume
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
),

-- TOP 10 address만 식별하고, 나머지는 모두 기타 처리로 묶어준다.
CTE_tx_volume_top10 AS (
    SELECT
        date,
        CASE
            WHEN rank <= 10 THEN 'Top' || LPAD(CAST(rank AS VARCHAR), 2, '0')
            ELSE '(Others)'
        END AS top10,
        SUM(tx_volume_token) AS tx_volume_token
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY date
                ORDER BY tx_volume_token DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS rank
        FROM
            CTE_without_invalid_addresses
    )
    GROUP BY
        1, 2
)
SELECT
    *
FROM
    CTE_tx_volume_top10
ORDER BY
    date,
    top10
;