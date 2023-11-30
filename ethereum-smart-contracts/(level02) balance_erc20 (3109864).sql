WITH
-- (1) TOKENS IN / TOKENS OUT 내역 모두 정리하기
CTE_in_out_by_date AS (
    SELECT
        DATE_TRUNC('DAY', datetime) AS date,
        contract_address,
        symbol,
        SUM(value_token) FILTER (
            WHERE to_address = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        ) AS tokens_in,
        SUM(value_token) FILTER (
            WHERE from_address = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        ) AS tokens_out
    FROM
        query_3109808 -- transfer_erc20
    GROUP BY
        1, 2, 3
),
-- (2) 일자별로 TOKENS 순 입금액 집계
CTE_net_transfers_by_date AS (
    SELECT
        *,
        COALESCE(tokens_in, 0) - COALESCE(tokens_out, 0) AS net_transfer_token
    FROM
        CTE_in_out_by_date
),
-- (3-1) Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Vector Table 만들기
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
-- (3-2) Transfer에 존재하는 모든 contract_address Vector Table 만들기
CTE_transfer_contract_addresses_frame AS (
    SELECT
        DISTINCT contract_address, symbol
    FROM
        CTE_net_transfers_by_date
),
-- (3-3) dates X contract_address CROSS JOIN된 Vector Table 만들기
CTE_transfer_frame AS (
    SELECT
        DATES.date,
        CONTRACTS.contract_address,
        CONTRACTS.symbol
    FROM
        CTE_transfer_dates_frame DATES
    CROSS JOIN
        CTE_transfer_contract_addresses_frame CONTRACTS
),
-- (4) 일자별로 현재 보유액 집계
CTE_running_balance AS (
    SELECT
        FRAME.date,
        FRAME.contract_address,
        FRAME.symbol,
        SUM(TRANSFERS.net_transfer_token) OVER (
            PARTITION BY FRAME.contract_address
            ORDER BY FRAME.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS balance_token
    FROM
        CTE_transfer_frame FRAME
    LEFT JOIN
        CTE_net_transfers_by_date TRANSFERS
        ON
            FRAME.date = TRANSFERS.date
            AND FRAME.contract_address = TRANSFERS.contract_address
),
-- (5) Close Price 값 추가해주기
CTE_running_balance_with_usd_prices AS (
    SELECT
        BALANCE.*,
        BALANCE.balance_token * USD.close_price AS balance_usd
    FROM
        CTE_running_balance BALANCE
    LEFT JOIN (
        SELECT
            contract_address,
            DATE_TRUNC('DAY', minute) AS date,
            LAST_VALUE(price) OVER (
                PARTITION BY contract_address, DATE_TRUNC('DAY', minute)
                ORDER BY minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS close_price,
            ROW_NUMBER() OVER (
                PARTITION BY contract_address, DATE_TRUNC('DAY', minute)
                ORDER BY minute DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS row_num
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
    ) USD
        ON BALANCE.date = USD.date AND BALANCE.contract_address = USD.contract_address
    WHERE
        row_num = 1
)
SELECT
    *
FROM
    CTE_running_balance_with_usd_prices
;