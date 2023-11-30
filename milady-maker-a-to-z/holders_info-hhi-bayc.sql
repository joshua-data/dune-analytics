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
        contract_address = 0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D -- Bored Ape Yacht Club
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
-- 일자별로 각 address의 NFT 순매수 개수 구하기 (매수 개수 - 매도 개수)
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
        SUM(COALESCE(B.net_buys, 0)) OVER (
            PARTITION BY F.address
            ORDER BY F.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_balance
    FROM CTE_frame F
    LEFT JOIN CTE_net_buys B
        ON F.date = B.evt_block_date AND F.address = B.address
    WHERE F.address NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    )
),
-- HHI 구하기
    -- HHI (Herfindal-Hershman Index)
    -- 공식
        -- H = SUM (각 주체의 점유율 ** 2)
    -- 의미
        -- 0에 가까울수록 집중도 낮음
        -- 1에 가까울수록 집중도 높음
    -- 미국의 합병심사시
        -- HHI < 0.10: 비집중적인 시장
        -- HHI <= 0.18: 어느 정도 집중적인 시장
        -- HHI > 0.18: 고도로 집중적인 시장
    -- 참고 자료
        -- https://www.ftc.go.kr/callPop.do?url=%2FjargonSearchView.do%3Fkey%3D451&dicseq=428&titl=%ED%97%88%ED%95%80%EB%8B%AC-%ED%97%88%EC%89%AC%EB%A7%8C+%EC%A7%80%EC%88%98%28Herfindal-Hershman+Index%29
-- ========
CTE_hhi_prepared AS (
    SELECT
        *,
        SUM(running_balance) OVER (PARTITION BY date) AS sum_running_balance,
        CAST(running_balance AS DOUBLE) / CAST(SUM(running_balance) OVER (PARTITION BY date) AS DOUBLE) AS running_balance_share
    FROM CTE_daily_running_balance
),
CTE_hhi AS (
    SELECT
        date,
        SUM(POWER(running_balance_share, 2)) AS hhi
    FROM CTE_hhi_prepared
    GROUP BY date
)
-- 완성
SELECT
    *,
    0.01 AS "0.01 Line",
    0.15 AS "0.15 Line",
    0.25 AS "0.25 Line"
FROM CTE_hhi
WHERE
    date > CAST('2021-08-24' AS DATE)
ORDER BY date
;