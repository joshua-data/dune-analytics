WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (
    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to"
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 -- Milady Maker CA
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
-- 오늘 기준으로 각 address의 NFT 현재 보유 개수 구하기
CTE_today_balance AS (
    SELECT
        address,
        SUM(COALESCE(net_buys, 0)) AS balance
    FROM CTE_net_buys
    GROUP BY
        address
),
-- NFT 1개 이상 보유한 address만 표시하기
CTE_holder_address AS (
    SELECT
        address
    FROM CTE_today_balance
    WHERE
        balance > 0
        AND address NOT IN (
            '0x0000000000000000000000000000000000000000',
            '0x000000000000000000000000000000000000dead'
        )
),
CTE_token_in AS (
    SELECT
        CAST("to" AS VARCHAR) AS address,
        SUM(CAST(value AS DOUBLE)) / POWER(10, 6) AS token_in
    FROM erc20_ethereum.evt_Transfer
    WHERE
        CAST("to" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
        AND CAST(value AS DOUBLE) > 0
    GROUP BY
        CAST("to" AS VARCHAR)
),
CTE_token_out AS (
    SELECT
        CAST("from" AS VARCHAR) AS address,
        SUM(CAST(value AS DOUBLE)) / POWER(10, 6) AS token_out
    FROM erc20_ethereum.evt_Transfer
    WHERE
        CAST("from" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
        AND CAST(value AS DOUBLE) > 0
    GROUP BY
        CAST("from" AS VARCHAR)    
),
CTE_summary AS (
    SELECT
        CONCAT(
            '<a href="https://etherscan.io/address/',
            A.address,
            '" target="_blank">',
            'Click</a>'            
        ) AS etherscan_link,
        A.address,
        TOKEN_IN.token_in, TOKEN_OUT.token_out,
        ROUND(COALESCE(TOKEN_IN.token_in, 0) - COALESCE(TOKEN_OUT.token_out, 0), 8) AS token_balance
    FROM CTE_holder_address A
    LEFT JOIN CTE_token_in TOKEN_IN ON A.address = TOKEN_IN.address
    LEFT JOIN CTE_token_out TOKEN_OUT ON A.address = TOKEN_OUT.address
),
CTE_added_usd AS (
    SELECT
        *,
        token_balance * (
            SELECT
                LAST_VALUE(price) IGNORE NULLS OVER (ORDER BY "minute" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as token_usd
            FROM prices.usd
            WHERE
                blockchain = 'ethereum'
                AND contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
                AND LOWER(symbol) = 'usdc'
            LIMIT 1
        ) token_balance_usd
    FROM CTE_summary
),
CTE_token_class AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY token_balance) AS perc_rank
    FROM CTE_added_usd
)
SELECT
    CASE
        WHEN perc_rank <= 0.10 THEN 'TOP 100%'
        WHEN perc_rank <= 0.20 THEN 'TOP 90%'
        WHEN perc_rank <= 0.30 THEN 'TOP 80%'
        WHEN perc_rank <= 0.40 THEN 'TOP 70%'
        WHEN perc_rank <= 0.50 THEN 'TOP 60%'
        WHEN perc_rank <= 0.60 THEN 'TOP 50%'
        WHEN perc_rank <= 0.70 THEN 'TOP 40%'
        WHEN perc_rank <= 0.80 THEN 'TOP 30%'
        WHEN perc_rank <= 0.90 THEN 'TOP 20%'
        WHEN perc_rank <= 1.00 THEN 'TOP 10%'
    END AS class,
    MIN(token_balance_usd) AS token_balance_usd_min,
    MAX(token_balance_usd) AS token_balance_usd_max,
    COUNT(address) AS address_cnt
FROM CTE_token_class
GROUP BY
    CASE
        WHEN perc_rank <= 0.10 THEN 'TOP 100%'
        WHEN perc_rank <= 0.20 THEN 'TOP 90%'
        WHEN perc_rank <= 0.30 THEN 'TOP 80%'
        WHEN perc_rank <= 0.40 THEN 'TOP 70%'
        WHEN perc_rank <= 0.50 THEN 'TOP 60%'
        WHEN perc_rank <= 0.60 THEN 'TOP 50%'
        WHEN perc_rank <= 0.70 THEN 'TOP 40%'
        WHEN perc_rank <= 0.80 THEN 'TOP 30%'
        WHEN perc_rank <= 0.90 THEN 'TOP 20%'
        WHEN perc_rank <= 1.00 THEN 'TOP 10%'
    END
ORDER BY
    token_balance_usd_min, token_balance_usd_max
;