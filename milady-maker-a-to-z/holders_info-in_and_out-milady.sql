WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (

    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to",
        tokenId
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 -- Milady Maker CA
),
-- Frame 만들기: (1) Dates Array 준비
CTE_frame_dates AS (
    SELECT
        CAST(DATE_COLUMN AS DATE) AS date
    FROM (
        SELECT 
            CAST(MIN(evt_block_date) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM CTE_raw
    ) AS date_limits
    CROSS JOIN UNNEST(SEQUENCE(date_limits.min_date, date_limits.max_date, INTERVAL '1' DAY)) AS T(DATE_COLUMN)
),
-- Frame 만들기: (2) Transfer에 참여한 모든 Addresses Array 준비
CTE_frame_addresses AS (
    SELECT DISTINCT address
    FROM (
        SELECT CAST("from" AS VARCHAR) AS address
        FROM CTE_raw
        UNION ALL
        SELECT CAST("to" AS VARCHAR) AS address
        FROM CTE_raw
    )
    WHERE address NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    )
),
-- Frame 만들기: (3) Frame 1과 Frame 2를 Cross Join해서 최종 Frame 완성
CTE_frame AS (
    SELECT
        D.date,
        A.address
    FROM CTE_frame_dates D
    CROSS JOIN CTE_frame_addresses A
),
-- 일자별로 to_address의 NFT 매수 개수 & from_address의 NFT 매도 개수 구하기
CTE_buys_sells AS (
    SELECT
        evt_block_date,
        CAST("to" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) * 1 AS transfer_cnt
    FROM CTE_raw
    GROUP BY
        evt_block_date, "to"
    UNION ALL
    SELECT
        evt_block_date,
        CAST("from" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) * -1 AS transfer_cnt
    FROM CTE_raw
    GROUP BY
        evt_block_date, "from"
),
-- 일자별로 각 address의 NFT 매수 & 매도 개수 구하기
CTE_net_buys AS (
    SELECT
        evt_block_date,
        address,
        SUM(transfer_cnt) AS net_buys
    FROM CTE_buys_sells
    GROUP BY
        evt_block_date, address
),
-- 일자별로 각 address의 NFT 현재 보유 개수 구하기
CTE_daily_running_balance AS (
    SELECT
        F.date,
        F.address,
        SUM(COALESCE(net_buys, 0)) OVER (
            PARTITION BY F.address
            ORDER BY F.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_balance
    FROM CTE_frame F
    LEFT JOIN CTE_net_buys B
        ON F.date = B.evt_block_date AND F.address = B.address
),
-- 일자별로 각 address가 신규 IN 했는지, 신규 OUT 했는지 표시해주기
CTE_daily_running_balance_in_out AS (
    SELECT
        date,
        address,
        running_balance,
        CASE
            WHEN
                LAG(running_balance, 1) OVER (PARTITION BY address ORDER BY date) = 0
                AND running_balance > 0
                THEN 'in' 
            WHEN
                LAG(running_balance, 1) OVER (PARTITION BY address ORDER BY date) > 0
                AND running_balance = 0
                THEN 'out'
            ELSE NULL
        END in_out_group
    FROM CTE_daily_running_balance
)
-- 완성: 일자별로 NFT를 1개 이상 보유한 address 수의 전일 대비 증감 수 구하기
SELECT
    date,
    COUNT(address) FILTER (WHERE in_out_group = 'in') * 1 AS holders_in_cnt,
    COUNT(address) FILTER (WHERE in_out_group = 'out') * -1 AS holders_out_cnt
FROM CTE_daily_running_balance_in_out
GROUP BY
    date
ORDER BY
    date
;