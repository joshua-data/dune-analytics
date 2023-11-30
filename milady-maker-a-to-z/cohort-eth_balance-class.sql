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

CTE_eth_in AS (
    SELECT
        CAST("to" AS VARCHAR) AS address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_in
    FROM ethereum.traces
    WHERE
        CAST("to" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND type = 'call'
        AND (
            call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
            OR call_type IS NULL
        )
        AND CAST(value AS DOUBLE) > 0
        AND success = True
    GROUP BY
        CAST("to" AS VARCHAR)
),
CTE_eth_out AS (
    SELECT
        CAST("from" AS VARCHAR) AS address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_out
    FROM ethereum.traces
    WHERE
        CAST("from" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND type = 'call'
        AND (
            call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
            OR call_type IS NULL
        )
        AND CAST(value AS DOUBLE) > 0
        AND success = True
    GROUP BY
        CAST("from" AS VARCHAR)
),
CTE_gas_spent AS (
    SELECT
        CAST(tx_sender AS VARCHAR) AS address,
        SUM(
            (gas_price_gwei * gas_used) / 1e9
        ) AS gas_spent
    FROM gas.fees
    WHERE
        blockchain = 'ethereum'
        AND CAST(tx_sender AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
    GROUP BY
        CAST(tx_sender AS VARCHAR)
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
        ETH_IN.eth_in, ETH_OUT.eth_out, GAS.gas_spent,
        ROUND(COALESCE(ETH_IN.eth_in, 0) - COALESCE(ETH_OUT.eth_out, 0) - COALESCE(GAS.gas_spent, 0), 8) AS eth_balance
    FROM CTE_holder_address A
    LEFT JOIN CTE_eth_in ETH_IN ON A.address = ETH_IN.address
    LEFT JOIN CTE_eth_out ETH_OUT ON A.address = ETH_OUT.address
    LEFT JOIN CTE_gas_spent GAS ON A.address = GAS.address
),
CTE_added_usd AS (
    SELECT
        *,
        eth_balance * (
            SELECT
                LAST_VALUE(price) IGNORE NULLS OVER (ORDER BY "minute" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as eth_usd
            FROM prices.usd
            WHERE
                blockchain = 'ethereum'
                AND contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
                AND LOWER(symbol) = 'weth'
            LIMIT 1
        ) eth_balance_usd
    FROM CTE_summary
),
CTE_eth_class AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY eth_balance) AS perc_rank
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
    COUNT(address) AS address_cnt
    MIN(eth_balance_usd) AS eth_balance_usd_min,
    MAX(eth_balance_usd) AS eth_balance_usd_max
FROM CTE_eth_class
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
    eth_balance_usd_min, eth_balance_usd_max
;