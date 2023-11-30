WITH
-- Transfer 최초 발생부터 오늘까지 1일 간격을 지닌 날짜 Array 가져오기
CTE_transfer_dates_frame AS (
    SELECT
        DATE_TRUNC('DAY', CAST(DATE_COLUMN AS DATE)) AS date
    FROM (
        SELECT
            CAST(MIN(date) AS DATE) AS min_date,
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
CTE_summary AS (
    SELECT
        FRAME.date,
        TRADES.counter_token_symbol,
        TRADES.counter_token_ca,
        COUNT(DISTINCT TRADES.tx_hash) FILTER (WHERE TRADES.trade_type = 'buy') AS txs_cnt_buy,
        COUNT(DISTINCT TRADES.tx_hash) FILTER (WHERE TRADES.trade_type = 'sell') * -1 AS txs_cnt_sell,
        SUM(TRADES.amount_token) FILTER (WHERE TRADES.trade_type = 'buy') AS tx_volume_token_buy,
        SUM(TRADES.amount_token) FILTER (WHERE TRADES.trade_type = 'sell') * -1 AS tx_volume_token_sell,
        SUM(TRADES.amount_usd) FILTER (WHERE TRADES.trade_type = 'buy') AS tx_volume_usd_buy,
        SUM(TRADES.amount_usd) FILTER (WHERE TRADES.trade_type = 'sell') * -1 AS tx_volume_usd_sell
    FROM
        CTE_transfer_dates_frame FRAME
    LEFT JOIN
        query_3104210 TRADES -- trades
        ON FRAME.date = TRADES.date
    GROUP BY
        1, 2, 3
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    date
;