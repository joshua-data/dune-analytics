WITH
-- 본 컨트랙트 계정이 from이거나 to인 모든 ETH Transfer 리스트 가져오기
CTE_transfer_eth AS (
    SELECT
        block_time AS datetime,
        CASE
            WHEN "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN "from"
            WHEN to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN to
        END AS ca_address,
        CASE
            WHEN "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN 'from'
            WHEN to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN 'to'
        END AS transfer_type,
        tx_hash,
        CASE
            WHEN "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN to
            WHEN to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN "from"
        END AS counter_address,
        CAST(value AS DOUBLE) / 1e18 AS value_eth,
        input AS data
    FROM
        ethereum.traces
    WHERE
        type = 'call'
        AND (
            call_type NOT IN ('delegatecall', 'staticcall', 'callcode')
            OR
            call_type IS NULL
        )
        AND success = True    
        AND (
            "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
            OR
            to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        )
),
-- 각 Transfer 내역에 Gas Spent 칼럼을 추가해주기
CTE_transfer_eth_with_gas_fee AS (
    SELECT
        MAIN.datetime,
        MAIN.ca_address,
        MAIN.transfer_type,
        MAIN.tx_hash,
        MAIN.counter_address,
        MAIN.value_eth,
        (GAS.gas_price_gwei * GAS.gas_used) / 1e9 AS gas_eth,
        MAIN.data
    FROM
        CTE_transfer_eth MAIN
    LEFT JOIN
        gas.fees GAS
        ON MAIN.tx_hash = GAS.tx_hash
)
SELECT
    *
FROM
    CTE_transfer_eth_with_gas_fee
;