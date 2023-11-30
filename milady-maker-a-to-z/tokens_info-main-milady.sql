WITH
CTE_raw AS (
    SELECT
        block_time,
        tx_hash,
        token_id,
        COALESCE(amount_usd, 0) AS amount_usd,
        COALESCE(platform_fee_amount_usd, 0) AS platform_fee_usd,
        COALESCE(royalty_fee_amount_usd, 0) AS royalty_fee_usd        
    FROM nft.trades
    WHERE
        blockchain = 'ethereum'
        AND nft_contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 -- Milady Maker CA
        AND tx_hash IS NOT NULL
        AND amount_usd IS NOT NULL
        AND CAST(tx_from AS VARCHAR) NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
        )
        AND CAST(tx_to AS VARCHAR) NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
        )
),
CTE_added_recent_price AS (
    SELECT
        *,
        LAST_VALUE(amount_usd) OVER (PARTITION BY token_id ORDER BY block_time) AS recent_amount_usd,
        LAST_VALUE(platform_fee_usd) OVER (PARTITION BY token_id ORDER BY block_time) AS recent_platform_fee_usd,
        LAST_VALUE(royalty_fee_usd) OVER (PARTITION BY token_id ORDER BY block_time) AS recent_royalty_fee_usd
    FROM CTE_raw
),
CTE_summary AS (
    SELECT
        -- Etherscan Link
        CONCAT(
            '<a href="https://etherscan.io/nft/0x5Af0D9827E0c53E4799BB226655A1de152A425a5/',
            CAST(token_id AS varchar),
            '" target="_blank">',
            'Click</a>'
        ) AS etherscan_link,
        token_id,
        -- Recent Trade Date
        CAST(MAX(block_time) AS DATE) AS recent_trade_date,
        -- Recent Price
        ROUND(MAX(recent_amount_usd), 2) AS recent_price_usd,
        -- All Time High & Low Price
        ROUND(MIN(amount_usd), 2) AS min_price_usd,
        ROUND(MAX(amount_usd), 2) AS max_price_usd,
        -- Price Volatility (High | Low)
        ROUND((MAX(amount_usd) / MIN(amount_usd) - 1), 4) AS price_rate_high_low,
        -- Price Volatility (Recent | Low)
        ROUND((MAX(recent_amount_usd) / MIN(amount_usd) - 1), 4) AS price_rate_recent_low,
        -- 거래량 & 거래대금
        COUNT(DISTINCT tx_hash) AS tx_count,
        ROUND(SUM(amount_usd), 2) AS tx_volume_usd
    FROM CTE_added_recent_price
    GROUP BY token_id
),
CTE_added_confidence_interval AS ( -- Z of 2.58 means 99% Confidence Level.
    SELECT
        *,
        AVG(min_price_usd) OVER() - 2.58 * (STDDEV_SAMP(min_price_usd) OVER()) / SQRT(COUNT(min_price_usd) OVER())
        AS confidence_lower,
        AVG(min_price_usd) OVER() + 2.58 * (STDDEV_SAMP(min_price_usd) OVER()) / SQRT(COUNT(min_price_usd) OVER())        
        AS confidence_upper
    FROM CTE_summary
)
SELECT
    *,
    CASE
        WHEN confidence_lower <= min_price_usd AND min_price_usd <= confidence_upper
            THEN 'Normal'
        ELSE 'Abnormal'
    END AS confidence_group
FROM CTE_added_confidence_interval
ORDER BY token_id;