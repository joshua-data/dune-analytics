WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (
    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to",
        CASE
            WHEN contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 THEN 'Milady Maker CA'
            WHEN contract_address = 0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB THEN 'Redacted Remilio Babies CA'
            WHEN contract_address = 0x8a45fb65311ac8434aad5b8a93d1eba6ac4e813b THEN 'Milady, That B.I.T.C.H.'
        END AS contract_name,
        tokenId
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address IN (
            0x5Af0D9827E0c53E4799BB226655A1de152A425a5, -- Milady Maker CA
            0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB, -- Redacted Remilio Babies CA
            0x8a45fb65311ac8434aad5b8a93d1eba6ac4e813b -- Milady, That B.I.T.C.H.
        )
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
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady Maker CA') AS transfer_cnt_milady,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Redacted Remilio Babies CA') AS transfer_cnt_remilio,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady, That B.I.T.C.H.') AS transfer_cnt_bitch
    FROM CTE_raw
    GROUP BY
        evt_block_date, "to"
    UNION ALL
    SELECT
        evt_block_date,
        CAST("from" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady Maker CA') * -1 AS transfer_cnt_milady,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Redacted Remilio Babies CA') * -1 AS transfer_cnt_remilio,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady, That B.I.T.C.H.') * -1 AS transfer_cnt_bitch
    FROM CTE_raw
    GROUP BY
        evt_block_date, "from"
),
-- 각 일자별 address의 NFT 보유 개수 구하기 (매수 개수 - 매도 개수)
CTE_net_buys AS (
    SELECT
        evt_block_date,
        address,
        SUM(transfer_cnt_milady) AS milady_cnt,
        SUM(transfer_cnt_remilio) AS remilio_cnt,
        SUM(transfer_cnt_bitch) AS bitch_cnt
    FROM CTE_buys_sells
    WHERE
        address != '0x0000000000000000000000000000000000000000'
    GROUP BY
        evt_block_date, address
),
-- 일자별로 각 address의 NFT 현재 보유 개수 구하기
CTE_daily_running_balance AS (
    SELECT
        F.date,
        F.address,
        SUM(COALESCE(milady_cnt, 0)) OVER (
            PARTITION BY F.address
            ORDER BY F.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_balance_milady,
        SUM(COALESCE(remilio_cnt, 0)) OVER (
            PARTITION BY F.address
            ORDER BY F.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_balance_remilio,
        SUM(COALESCE(bitch_cnt, 0)) OVER (
            PARTITION BY F.address
            ORDER BY F.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_balance_bitch
    FROM CTE_frame F
    LEFT JOIN CTE_net_buys B
        ON F.date = B.evt_block_date AND F.address = B.address
)
-- 완성
SELECT
    date,
    COUNT(DISTINCT address) FILTER (WHERE running_balance_milady > 0 AND running_balance_remilio = 0 AND running_balance_bitch = 0) AS milady,
    COUNT(DISTINCT address) FILTER (WHERE running_balance_milady > 0 AND running_balance_remilio = 0 AND running_balance_bitch > 0) AS milady_bitch,
    COUNT(DISTINCT address) FILTER (WHERE running_balance_milady > 0 AND running_balance_remilio > 0 AND running_balance_bitch = 0) AS milady_remilio,
    COUNT(DISTINCT address) FILTER (WHERE running_balance_milady > 0 AND running_balance_remilio > 0 AND running_balance_bitch > 0) AS milady_remilio_bitch
FROM CTE_daily_running_balance
GROUP BY
    date
ORDER BY
    date
;