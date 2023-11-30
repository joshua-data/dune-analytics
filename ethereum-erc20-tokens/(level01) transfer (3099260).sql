WITH
-- 기준 ERC-20 토큰의 CA Address, Symbol, Decimal 값 가져오기
CTE_token_info AS (
    SELECT
        contract_address,
        symbol,
        decimals
    FROM
        tokens.erc20
    WHERE
        blockchain = 'ethereum'
        AND contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
),
-- 기준 ERC-20 토큰의 Transfer 이력 모두 가져오기
CTE_transfer AS (
    SELECT
        TRANSFERS.evt_block_time AS datetime,
        TRANSFERS.evt_tx_hash AS tx_hash,
        INFO.contract_address AS token_ca,
        INFO.symbol AS token_symbol,
        TRANSFERS."from" AS from_address,
        TRANSFERS.to AS to_address,
        TRANSFERS.value / POWER(10, INFO.decimals) AS value_token
    FROM
        erc20_ethereum.evt_Transfer TRANSFERS
    LEFT JOIN
        CTE_token_info INFO
        ON TRANSFERS.contract_address = INFO.contract_address
    WHERE
        INFO.contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
        AND TRANSFERS.evt_tx_hash IS NOT NULL
        AND TRANSFERS."from" IS NOT NULL
        AND TRANSFERS.to IS NOT NULL
)
SELECT
    *
FROM
    CTE_transfer
;