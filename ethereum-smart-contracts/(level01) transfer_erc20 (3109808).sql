WITH
-- 모든 ERC-20 토큰의 CA Address, Symbol, Decimal 값 가져오기
CTE_info AS (
    SELECT
        DISTINCT
        contract_address,
        symbol,
        decimals
    FROM
        tokens.erc20
    WHERE
        blockchain = 'ethereum'
),
-- (1) 본 컨트랙트 계정이 from이거나 to인 모든 ERC-20 Transfer 리스트 가져오기
CTE_transfer_erc20 AS (
    SELECT
        TRANSFERS.evt_block_time AS datetime,
        CASE
            WHEN TRANSFERS."from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN TRANSFERS."from"
            WHEN TRANSFERS.to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN TRANSFERS.to
        END AS ca_address,
        CASE
            WHEN TRANSFERS."from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN 'from'
            WHEN TRANSFERS.to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN 'to'
        END AS transfer_type,        
        TRANSFERS.evt_tx_hash AS tx_hash,
        CASE
            WHEN TRANSFERS."from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN to
            WHEN TRANSFERS.to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c THEN "from"
        END AS AS counter_address,
        TRANSFERS.contract_address AS token_ca,
        INFO.symbol AS token_symbol,
        TRANSFERS.value / POWER(10, INFO.decimals) AS value_token
    FROM
        erc20_ethereum.evt_Transfer TRANSFERS
    LEFT JOIN
        CTE_info INFO
        ON TRANSFERS.contract_address = INFO.contract_address
    WHERE
        TRANSFERS.evt_tx_hash IS NOT NULL
        AND TRANSFERS."from" IS NOT NULL
        AND TRANSFERS.to IS NOT NULL
        AND (
            TRANSFERS."from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
            OR
            TRANSFERS.to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        )
)
SELECT
    *
FROM
    CTE_transfer_erc20
;