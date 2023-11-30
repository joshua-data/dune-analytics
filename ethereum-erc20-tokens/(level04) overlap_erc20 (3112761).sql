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
-- (1) 모든 ERC-20 토큰 IN 내역
CTE_tokens_in AS (
    SELECT
        HOLDERS.address,
        INFO.contract_address,
        INFO.symbol,
        SUM(TRANSFERS.value / POWER(10, INFO.decimals)) AS tokens_in
    FROM
        CTE_holders HOLDERS
    LEFT JOIN
        erc20_ethereum.evt_Transfer TRANSFERS
        ON TRANSFERS.to = HOLDERS.address
    LEFT JOIN
        CTE_info INFO
        ON TRANSFERS.contract_address = INFO.contract_address
    WHERE
        TRANSFERS.evt_tx_hash IS NOT NULL
        AND TRANSFERS."from" IS NOT NULL
    GROUP BY
        1, 2, 3
),
-- (2) 모든 ERC-20 토큰 OUT 내역
CTE_tokens_out AS (
    SELECT
        HOLDERS.token_ca,
        HOLDERS.token_symbol,
        HOLDERS.address,
        INFO.contract_address,
        INFO.symbol,
        SUM(TRANSFERS.value / POWER(10, INFO.decimals)) AS tokens_out
    FROM
        CTE_holders HOLDERS
    LEFT JOIN
        erc20_ethereum.evt_Transfer TRANSFERS
        ON TRANSFERS."from" = HOLDERS.address
    LEFT JOIN
        CTE_info INFO
        ON TRANSFERS.contract_address = INFO.contract_address
    WHERE
        TRANSFERS.evt_tx_hash IS NOT NULL
        AND TRANSFERS.to IS NOT NULL
    GROUP BY
        1, 2, 3, 4, 5
),
-- (0+1+2) 모든 ERC-20 토큰 현재 Balance 집계
CTE_tokens_balance_by_address AS (
    SELECT
        HOLDERS.token_ca,
        HOLDERS.token_symbol,
        HOLDERS.address,
        TOKENS_IN.contract_address,
        TOKENS_IN.symbol,
        COALESCE(TOKENS_IN.tokens_in, 0) - COALESCE(TOKENS_OUT.tokens_out, 0) AS balance_token
    FROM
        CTE_holders HOLDERS
    LEFT JOIN
        CTE_tokens_in TOKENS_IN
        ON HOLDERS.address = TOKENS_IN.address
    LEFT JOIN
        CTE_tokens_out TOKENS_OUT
        ON
            HOLDERS.address = TOKENS_OUT.address
            AND TOKENS_IN.contract_address = TOKENS_OUT.contract_address
    WHERE
        COALESCE(TOKENS_IN.tokens_in, 0) - COALESCE(TOKENS_OUT.tokens_out, 0) > 0
),
-- 모든 ERC-20 토큰 현재 Balance 집계 (USD 환산액 칼럼 추가)
CTE_tokens_balance_by_address_with_usd AS (
    SELECT
        BALANCE.token_ca,
        BALANCE.token_symbol,
        BALANCE.address,
        BALANCE.contract_address,
        BALANCE.symbol,
        BALANCE.balance_token,
        BALANCE.balance_token * USD.last_price AS balance_usd
    FROM
        CTE_tokens_balance_by_address BALANCE
    LEFT JOIN (
        SELECT
            contract_address,
            LAST_VALUE(price) OVER (
                PARTITION BY contract_address
                ORDER BY minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_price,
            ROW_NUMBER() OVER (
                PARTITION BY contract_address
                ORDER BY minute DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS row_num
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
    ) USD
        ON BALANCE.contract_address = USD.contract_address
    WHERE
        row_num = 1
)
SELECT
    *
FROM
    CTE_tokens_balance_by_address_with_usd
;