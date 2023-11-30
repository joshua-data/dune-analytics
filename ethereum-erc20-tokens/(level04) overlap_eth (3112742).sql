WITH
-- (0) 홀더 리스트 먼저 가져오기
CTE_holders AS (
    SELECT
        token_ca,
        token_symbol,
        address
    FROM
        query_3104364 -- holders
),
-- (1) ETH IN 내역
CTE_eth_in AS (
    SELECT
        to AS address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_in
    FROM
        ethereum.traces
    WHERE
        type = 'call'
        AND (
            call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
            OR
            call_type IS NULL
        )
        AND CAST(value AS DOUBLE) > 0
        AND success = True
        AND to IN (SELECT address FROM CTE_holders)
    GROUP BY
        1
),
-- (2) ETH OUT 내역
CTE_eth_out AS (
    SELECT
        "from" AS address,
        SUM(CAST(value AS DOUBLE) / 1e18) AS eth_out
    FROM
        ethereum.traces
    WHERE
        type = 'call'
        AND (
            call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
            OR
            call_type IS NULL
        )
        AND CAST(value AS DOUBLE) > 0
        AND success = True
        AND "from" IN (SELECT address FROM CTE_holders)
    GROUP BY
        1
),
-- (3) Gas Spent 내역
CTE_gas_spent AS (
    SELECT
        tx_sender AS address,
        SUM((gas_price_gwei * gas_used) / 1e9) AS gas_spent
    FROM
        gas.fees
    WHERE
        blockchain = 'ethereum'
        AND tx_sender IN (SELECT address FROM CTE_holders)
    GROUP BY
        1
),
-- (0+1+2+3) ETH 현재 Balance 집계
CTE_eth_balance_by_address AS (
    SELECT
        HOLDERS.token_ca,
        HOLDERS.token_symbol,
        HOLDERS.address,
        COALESCE(ETH_IN.eth_in, 0) - COALESCE(ETH_OUT.eth_out, 0) - COALESCE(GAS.gas_spent, 0) as balance_eth
    FROM
        CTE_holders HOLDERS
    LEFT JOIN
        CTE_eth_in ETH_IN
        ON HOLDERS.address = ETH_IN.address
    LEFT JOIN
        CTE_eth_out ETH_OUT
        ON HOLDERS.address = ETH_OUT.address
    LEFT JOIN
        CTE_gas_spent GAS
        ON HOLDERS.address = GAS.address
    WHERE
        COALESCE(ETH_IN.eth_in, 0) - COALESCE(ETH_OUT.eth_out, 0) - COALESCE(GAS.gas_spent, 0) > 0
)
SELECT
    *
FROM
    CTE_eth_balance_by_address
;