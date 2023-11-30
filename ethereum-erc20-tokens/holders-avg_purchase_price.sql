WITH
-- from_address & to_address를 unpivot하고, 입금 vs 송금 여부로 구분하여 재집계한다.
CTE_transfer_unpivot AS (
    SELECT
        date,
        to_address AS address,
        'buy' AS transfer_type,
        value_token * 1 AS transfer_token
    FROM query_3099260 -- transfer
    UNION ALL
    SELECT
        date,
        from_address AS address,
        'sell' AS transfer_type,
        value_token * -1 AS transfer_token
    FROM query_3099260 -- transfer
),
-- USD Price 정보를 Label해준다. (일자별 Close Price 기준)
CTE_with_usd_prices AS (
    SELECT
        TRANSFER.date,
        TRANSFER.address,
        TRANSFER.transfer_type,
        TRANSFER.transfer_token,
        USD.close_price,
        TRANSFER.transfer_token * USD.close_price AS transfer_usd
    FROM    
        CTE_transfer_unpivot TRANSFER
    LEFT JOIN (
        SELECT
            DATE_TRUNC('DAY', minute) AS date,
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
        ON TRANSFER.date = USD.date
    GROUP BY
        1, 2, 3, 4, 5, 6
),
-- 각 address별로 현재 보유액과 평균매수단가를 요약한다.
CTE_summary_by_addresses AS (
    SELECT
        address,
        SUM(transfer_token) AS balance_token,
        SUM(transfer_usd) FILTER (WHERE transfer_type = 'buy')
        /
        SUM(transfer_token) FILTER (WHERE transfer_type = 'buy') AS avg_purchase_price_usd
    FROM
        CTE_with_usd_prices
    GROUP BY
        address
),
-- 표시할 필요가 없는 address를 제거한다.
CTE_without_invalid_addresses AS (
    SELECT
        *
    FROM
        CTE_summary_by_addresses
    WHERE
        address NOT IN (
            0x0000000000000000000000000000000000000000,
            0x000000000000000000000000000000000000dead
        )
        AND balance_token > 0
),
-- 분포도를 시각화하기 위한 작업을 해준다.
CTE_summary AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY avg_purchase_price_usd
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS address_idx,
        balance_token,
        avg_purchase_price_usd
    FROM
        CTE_without_invalid_addresses
    WHERE
        avg_purchase_price_usd IS NOT NULL
)
SELECT
    *,
    CAST(address_idx AS DOUBLE) / MAX(address_idx) OVER (
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS address_idx_cumulative_dist
FROM
    CTE_summary
ORDER BY
    address_idx
;