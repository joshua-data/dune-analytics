WITH
-- 가장 최근 날짜 기준으로 balance > 0 인 address 리스트를 가져온다.
CTE_current_balance AS (
    SELECT
        token_ca,
        token_symbol,
        address,
        balance_token
    FROM (
        SELECT
            token_ca,
            token_symbol,
            address,
            balance_token,
            ROW_NUMBER() OVER (
                PARTITION BY token_ca, token_symbol, address
                ORDER BY date DESC
            ) AS row_num
        FROM
            query_3101913 -- balance
    )
    WHERE
        row_num = 1
        AND balance_token > 0
),
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        CTE_current_balance
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