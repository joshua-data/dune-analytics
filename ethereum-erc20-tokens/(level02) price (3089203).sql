WITH
-- Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Vector Table 만들기
CTE_transfer_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(datetime) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM query_3099260 -- transfer
    ) AS DATE_LIMITS
    CROSS JOIN
        UNNEST(
            SEQUENCE(
                DATE_LIMITS.min_date,
                DATE_LIMITS.max_date,
                INTERVAL '1' DAY
            )
        ) AS T(DATE_COLUMN)
),
-- 일자별로 Close Price 값 가져오기
CTE_usd_prices AS (
    SELECT
        FRAME.date,
        USD.contract_address AS token_ca,
        USD.symbol AS token_symbol,
        USD.close_price
    FROM
        CTE_transfer_dates_frame FRAME
    LEFT JOIN (
        SELECT
            DATE_TRUNC('DAY', minute) AS date,
            contract_address,
            symbol,
            LAST_VALUE(price) OVER (
                PARTITION BY DATE_TRUNC('DAY', minute)
                ORDER BY minute
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS close_price
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
            AND contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
    ) USD
        ON FRAME.date = USD.date
    GROUP BY
        1, 2, 3, 4
)
SELECT
    *
FROM
    CTE_usd_prices
;