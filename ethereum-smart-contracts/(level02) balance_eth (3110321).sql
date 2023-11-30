WITH
-- (1) ETH IN / ETH OUT / Gas Spent 내역 모두 정리하기
CTE_in_out_by_date AS (
    SELECT
        DATE_TRUNC('DAY', datetime) AS date,
        SUM(value_eth) FILTER (
            WHERE to_address = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        ) AS eth_in,
        SUM(value_eth) FILTER (
            WHERE from_address = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        ) AS eth_out,
        SUM(gas_eth) FILTER (
            WHERE from_address = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c            
        ) AS gas_eth
    FROM
        query_3120339 -- transfer_eth
    GROUP BY
        1
),
-- (2) 일자별로 ETH 순 입금액 집계
CTE_net_transfers_by_date AS (
    SELECT
        *,
        COALESCE(eth_in, 0) - COALESCE(eth_out, 0) - COALESCE(gas_eth, 0) AS net_transfer_eth
    FROM
        CTE_in_out_by_date
),
-- (3) Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Vector Table 만들기
CTE_transfer_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(date) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM CTE_net_transfers_by_date
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
-- (4) 일자별로 현재 보유액 집계
CTE_running_balance AS (
    SELECT
        FRAME.date,
        SUM(TRANSFERS.net_transfer_eth) OVER (
            ORDER BY FRAME.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_eth
    FROM
        CTE_transfer_dates_frame FRAME
    LEFT JOIN
        CTE_net_transfers_by_date TRANSFERS
        ON FRAME.date = TRANSFERS.date
)
SELECT
    *
FROM
    CTE_running_balance
;