WITH
CTE_trades AS (
    SELECT
        -- =================== 기본 정보
        block_date AS datetime,
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN 'buy'
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN 'sell'
        END AS trade_type, -- 기준 토큰을 Buy한 건지, Sell한 건지 구분
        project, -- DEX 플랫폼 이름
        tx_hash, -- TXID
        tx_from AS address, -- 거래를 하는 사용자의 지갑 주소
        -- =================== 기준 토큰 정보
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_bought_address
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_address
        END AS token_ca, -- 기준 토큰의 CA Address
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_bought_symbol
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_symbol
        END AS token_symbol, -- 기준 토큰의 Symbol
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_bought_amount
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_amount
        END AS amount_token, -- 기준 토큰의 거래액 (단위: 토큰)
        amount_usd, -- 기준 토큰의 거래액 (단위: USD)
        -- =================== 기준 토큰을 댓가로 거래하는 토큰 정보
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_address
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_address
        END AS counter_token_ca, -- 기준 토큰의 댓가로 거래하는 토큰의 CA Address
        CASE
            WHEN token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_sold_symbol
            WHEN token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44 THEN token_bought_symbol
        END AS counter_token_symbol -- 기준 토큰을 댓가로 거래하는 토큰의 Symbol
    FROM
        dex.trades
    WHERE
        blockchain = 'ethereum'
        AND token_bought_address IS NOT NULL
        AND token_sold_address IS NOT NULL
        AND (
            token_bought_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
            OR token_sold_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
        )
        AND tx_hash IS NOT NULL
        AND tx_from IS NOT NULL
),
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        CTE_trades
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
)
SELECT
    *
FROM
    CTE_without_invalid_addresses
;