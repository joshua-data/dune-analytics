WITH
CTE_summary AS (
    SELECT
        TRADES.counter_token_symbol,
        TRADES.counter_token_ca,
        COUNT(DISTINCT TRADES.tx_hash) FILTER (WHERE TRADES.trade_type = 'buy') AS txs_cnt_buy,
        COUNT(DISTINCT TRADES.tx_hash) FILTER (WHERE TRADES.trade_type = 'sell') AS txs_cnt_sell,
        SUM(TRADES.amount_token) FILTER (WHERE TRADES.trade_type = 'buy') AS tx_volume_token_buy,
        SUM(TRADES.amount_token) FILTER (WHERE TRADES.trade_type = 'sell') AS tx_volume_token_sell,
        SUM(TRADES.amount_usd) FILTER (WHERE TRADES.trade_type = 'buy') AS tx_volume_usd_buy,
        SUM(TRADES.amount_usd) FILTER (WHERE TRADES.trade_type = 'sell') AS tx_volume_usd_sell
    FROM
        query_3104210 TRADES -- trades
    GROUP BY
        1, 2
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    tx_volume_usd_buy DESC,
    tx_volume_usd_sell DESC
;