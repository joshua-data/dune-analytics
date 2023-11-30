WITH
-- Transfer의 from 혹은 to인 모든 address 리스트 가져오기
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
-- Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Vector Table 만들기
CTE_transfer_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(datetime) AS DATE) AS min_date,
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
-- 1일 간격 날짜 X Transfer 참여한 address 크로스 조인하여 Vector Table 만들기
CTE_transfer_frame AS (
    SELECT
        DATES.date,
        ADDRESSES.address
    FROM
        CTE_transfer_dates_frame DATES
    CROSS JOIN
        CTE_transfer_addresses_frame ADDRESSES
),
-- 일자별로 각 address마다 순 입금액 (입금액 - 출금액) 집계
CTE_net_transfers_by_date_address AS (
    SELECT
        date,
        token_ca,
        token_symbol,
        address,
        SUM(transfer_token) AS net_transfers_token
    FROM (
        SELECT
            CAST(datetime AS DATE) AS date,
            token_ca,
            token_symbol,
            to_address AS address,
            SUM(value_token) * 1 AS transfer_token
        FROM query_3099260 -- transfer
        GROUP BY 1, 2, 3, 4
        UNION ALL
        SELECT
            CAST(datetime AS DATE) AS date,
            token_ca,
            token_symbol,
            from_address AS address,
            SUM(value_token) * -1 AS transfer_token
        FROM query_3099260 -- transfer
        GROUP BY 1, 2, 3, 4    
    )
    GROUP BY
        1, 2, 3, 4
),
-- 일자별로 각 address마다 현재 보유액 집계
CTE_running_balance AS (
    SELECT
        FRAME.date,
        TRANSFERS.token_ca,
        TRANSFERS.token_symbol,
        FRAME.address,
        SUM(net_transfers_token) OVER (
            PARTITION BY TRANSFERS.token_ca, TRANSFERS.token_symbol, FRAME.address
            ORDER BY FRAME.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_token
    FROM 
        CTE_transfer_frame FRAME
    LEFT JOIN
        CTE_net_transfers_by_date_address TRANSFERS
        ON FRAME.date = TRANSFERS.date AND FRAME.address = TRANSFERS.address 
)
SELECT
    *
FROM
    CTE_running_balance
;