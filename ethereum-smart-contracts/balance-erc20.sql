WITH
CTE_summary AS (
    SELECT
        date,
        contract_address,
        CASE
            WHEN symbol IS NULL THEN CAST(contract_address AS VARCHAR)
            ELSE symbol
        END AS symbol,
        COALESCE(balance_token, 0) AS balance_token,
        COALESCE(balance_usd, 0) AS balance_usd
    FROM
        query_3109864 -- balance_erc20
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    date,
    contract_address
;