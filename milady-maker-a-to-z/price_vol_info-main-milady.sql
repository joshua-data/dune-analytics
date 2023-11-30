WITH
CTE_raw AS (
    SELECT
        CAST(block_date AS DATE) AS block_date,
        tx_hash,
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
-- Dates Array 준비
CTE_frame_dates AS (
    SELECT
        CAST(DATE_COLUMN AS DATE) AS block_date
    FROM (
        SELECT 
            CAST(MIN(block_date) AS DATE) AS min_date,
            CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS max_date
        FROM CTE_raw
    ) AS date_limits
    CROSS JOIN UNNEST(SEQUENCE(date_limits.min_date, date_limits.max_date, INTERVAL '1' DAY)) AS T(DATE_COLUMN)    
),
CTE_summary AS (
    SELECT
        CAST(F.block_date AS DATE) AS block_date,
        -- 거래 가격 (USD)
        MIN(amount_usd) AS min_price_usd,
        APPROX_PERCENTILE(amount_usd, 0.5) AS median_price_usd,
        MAX(amount_usd) AS max_price_usd,
        -- 거래량 & 거래대금
        COUNT(DISTINCT tx_hash) AS tx_count,
        SUM(amount_usd) AS tx_volume_usd
    FROM CTE_raw R
    RIGHT JOIN CTE_frame_dates F
        ON R.block_date = F.block_date
    GROUP BY F.block_date
),
-- NULL Price가 있으면, 가장 최근의 Non-null Price로 Fill 해주기
CTE_fill_null_prices AS (
    SELECT
        block_date,
        IF(min_price_usd IS NOT NULL, min_price_usd, LAST_VALUE(min_price_usd) IGNORE NULLS OVER (ORDER BY block_date)) AS min_price_usd,
        IF(median_price_usd IS NOT NULL, median_price_usd, LAST_VALUE(median_price_usd) IGNORE NULLS OVER (ORDER BY block_date)) AS median_price_usd,
        IF(max_price_usd IS NOT NULL, max_price_usd, LAST_VALUE(max_price_usd) IGNORE NULLS OVER (ORDER BY block_date)) AS max_price_usd,
        tx_count,
        tx_volume_usd
    FROM CTE_summary
)
-- min_price_eth, min_price_usd 의 이동평균 계산하여 완료하기
SELECT
    *,
    AVG(min_price_usd) OVER (
        ORDER BY block_date
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS min_price_usd_ma_7d,
    AVG(min_price_usd) OVER (
        ORDER BY block_date
        ROWS BETWEEN 28 PRECEDING AND CURRENT ROW
    ) AS min_price_usd_ma_28d,
    AVG(min_price_usd) OVER (
        ORDER BY block_date
        ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
    ) AS min_price_usd_ma_90d
FROM CTE_fill_null_prices
ORDER BY block_date DESC;