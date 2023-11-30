WITH
CTE_marketcap AS (
    SELECT
        USD.date,
        COALESCE(USD.close_price, 0) * COALESCE(SUPPLY.total_supply, 0) AS marketcap        
    FROM
        query_3089203 USD -- price
    LEFT JOIN
        query_3104182 SUPPLY -- supply_mint_burn
        ON USD.date = SUPPLY.date
)
SELECT
    *
FROM
    CTE_marketcap
ORDER BY
    date
;