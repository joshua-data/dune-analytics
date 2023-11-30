WITH
-- ETH IN 내역
CTE_eth_in AS (
    SELECT
        HOLDERS.address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_in
    FROM
        query_3104364 HOLDERS -- holders
    LEFT JOIN
        ethereum.traces ETH
        ON HOLDERS.address = ETH.to
            AND ETH.type = 'call'
            AND (
                ETH.call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
                OR ETH.call_type IS NULL
            )
            AND CAST(ETH.value AS DOUBLE) > 0
            AND ETH.success = True
    GROUP BY
        HOLDERS.address
),
-- ETH OUT 내역
CTE_eth_out AS (
    SELECT
        HOLDERS.address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_out
    FROM
        query_3104364 HOLDERS -- holders
    LEFT JOIN
        ethereum.traces ETH
        ON HOLDERS.address = ETH."from"
            AND ETH.type = 'call'
            AND (
                ETH.call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
                OR ETH.call_type IS NULL
            )
            AND CAST(ETH.value AS DOUBLE) > 0
            AND ETH.success = True
    GROUP BY
        HOLDERS.address
),
-- Gas Spent 내역
CTE_gas_spent AS (
    SELECT
        HOLDERS.address,
        SUM(
            (GAS.gas_price_gwei * GAS.gas_used) / 1e9
        ) AS gas_spent
    FROM
        query_3104364 HOLDERS -- holders
    LEFT JOIN
        gas.fees GAS
        ON HOLDERS.address = GAS.tx_sender
            AND GAS.blockchain = 'ethereum'
    GROUP BY
        HOLDERS.address
),
-- 집계 시작
CTE_eth_balance AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(ETH_IN.eth_in, 0)
                - COALESCE(ETH_OUT.eth_out, 0)
                - COALESCE(GAS.gas_spent, 0)
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS address_idx,
        ETH_IN.address,
        ETH_IN.eth_in,
        ETH_OUT.eth_out,
        GAS.gas_spent,
        ROUND(
            COALESCE(ETH_IN.eth_in, 0)
            - COALESCE(ETH_OUT.eth_out, 0)
            - COALESCE(GAS.gas_spent, 0),
            8
        ) AS balance_eth
    FROM
        CTE_eth_in ETH_IN
    LEFT JOIN
        CTE_eth_out ETH_OUT
        ON ETH_IN.address = ETH_OUT.address
    LEFT JOIN
        CTE_gas_spent GAS
        ON ETH_IN.address = GAS.address
),
-- 분포도를 시각화하기 위한 작업을 해준다.
CTE_summary AS (
    SELECT
        CAST(address_idx AS DOUBLE) / MAX(address_idx) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS address_idx_cumulative_dist,
        *
    FROM
        CTE_eth_balance
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    address_idx
;