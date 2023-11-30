WITH
-- 이동 평균 추가해주기
CTE_usd_prices_with_ma AS (
    SELECT
        *,
        AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ma_5d,
        AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS ma_10d,
        AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS ma_20d,
        AVG(close_price) OVER (ORDER BY date ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS ma_60d
    FROM
        query_3089203 -- price
)
SELECT
    *
FROM
    CTE_usd_prices_with_ma
ORDER BY
    date
;