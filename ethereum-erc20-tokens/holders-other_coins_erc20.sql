WITH
-- 해당 ERC-20 토큰의 decimal 값 가져오기
CTE_decimals AS (
    SELECT
        decimals
    FROM
        tokens.erc20
    WHERE
        blockchain = 'ethereum'
        AND contract_address = {{erc20_token_address}}
    LIMIT
        1
),
-- 해당 ERC-20 토큰의 Transfer 이력 모두 가져오기
CTE_transfer AS (
    SELECT
        DATE_TRUNC('DAY', evt_block_time) AS date,
        evt_tx_hash,
        "from" AS from_address,
        to AS to_address,
        value / POWER(10, (SELECT decimals FROM CTE_decimals)) AS value_token
    FROM
        erc20_ethereum.evt_Transfer
    WHERE
        contract_address = {{erc20_token_address}}
        AND evt_tx_hash IS NOT NULL
        AND "from" IS NOT NULL
        AND to IS NOT NULL
),
-- 각 address마다 순 입금액 (입금액 - 출금액) 집계
CTE_current_balance_by_address AS (
    SELECT
        HOLDERS.address,
        SUM(TRANSFER.transfer_token) AS balance_token
    FROM
        query_3104364 HOLDERS -- holders
    LEFT JOIN (
        SELECT
            to_address AS address,
            SUM(value_token) * 1 AS transfer_token
        FROM CTE_transfer
        GROUP BY to_address
        UNION ALL
        SELECT
            from_address AS address,
            SUM(value_token) * -1 AS transfer_token
        FROM CTE_transfer
        GROUP BY from_address
    ) TRANSFER
        ON HOLDERS.address = TRANSFER.address
    GROUP BY
        1
),
-- Close Price 값 가져오기
CTE_with_usd_prices AS (
    SELECT
        BALANCE.address,
        BALANCE.balance_token,
        BALANCE.balance_token * (
            SELECT
                LAST_VALUE(price) OVER (
                    ORDER BY minute DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS current_price
            FROM
                prices.usd
            WHERE
                blockchain = 'ethereum'
                AND contract_address = {{erc20_token_address}}
            LIMIT
                1
        ) AS balance_usd
    FROM
        CTE_current_balance_by_address BALANCE
),
-- 최종 정리
CTE_rounded AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY COALESCE(balance_token, 0)
        ) AS address_idx,
        address,
        ROUND(COALESCE(balance_token, 0), 8) AS balance_token,
        ROUND(COALESCE(balance_usd, 0), 2) AS balance_usd
    FROM
        CTE_with_usd_prices
),
-- 분포도를 시각화하기 위한 작업을 해준다.
CTE_summary AS (
    SELECT
        CAST(address_idx AS DOUBLE) / MAX(address_idx) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS address_idx_cumulative_dist,
        *
    FROM
        CTE_rounded
)
SELECT
    *
FROM
    CTE_summary
ORDER BY
    address_idx
;