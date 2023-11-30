WITH
-- 일자별로 누적 총공급량, 누적 총발행량, 누적 총소각량 값 가져오기
CTE_supply_mint_burn AS (
    SELECT
        date,
        token_ca,
        token_symbol,
        SUM(COALESCE(balance_token, 0)) FILTER (
            WHERE address NOT IN (
                0x0000000000000000000000000000000000000000,
                0x000000000000000000000000000000000000dead                
            )
        ) AS total_supply,
        SUM(COALESCE(balance_token, 0)) FILTER (
            WHERE address = 0x0000000000000000000000000000000000000000
        ) * -1 AS total_mint, -- 양수로 바꿔주기 위해 -1 곱함
        SUM(COALESCE(balance_token, 0)) FILTER (
            WHERE address = 0x000000000000000000000000000000000000dead
        ) * -1 AS total_burn -- 음수로 바꿔주기 위해 -1 곱함
    FROM
        query_3101913 -- balance
    GROUP BY
        1, 2, 3
)
SELECT
    *
FROM
    CTE_supply_mint_burn
;