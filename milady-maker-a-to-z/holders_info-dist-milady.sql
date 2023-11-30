WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (
    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to",
        tokenId
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 -- Milady Maker CA
),
-- 일자별로 to_address의 NFT 매수 개수 & from_address의 NFT 매도 개수 구하기
CTE_buys_sells AS (
    SELECT
        evt_block_date,
        CAST("to" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) AS transfer_cnt
    FROM CTE_raw
    GROUP BY
        evt_block_date, "to"
    UNION ALL
    SELECT
        evt_block_date,
        CAST("from" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) * -1 AS transfer_cnt
    FROM CTE_raw
    GROUP BY
        evt_block_date, "from"
),
-- 각 address의 NFT 보유 개수 구하기 (매수 개수 - 매도 개수)
CTE_net_buys AS (
    SELECT
        address,
        SUM(transfer_cnt) AS nfts_cnt
    FROM CTE_buys_sells
    WHERE address NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    )
    GROUP BY
        address
),
-- NFT 보유 개수별 address의 수 구하기
CTE_summary AS (
    SELECT
        nfts_cnt,
        COUNT(address) AS address_cnt
    FROM CTE_net_buys
    GROUP BY
        nfts_cnt
    HAVING
        nfts_cnt > 0
),
-- Frame 만들기: NFTs Count 준비
CTE_frame AS (
    SELECT
        CAST(CNT_COLUMN AS INTEGER) AS nfts_cnt
    FROM (
        SELECT
            MIN(nfts_cnt) AS min_cnt,
            MAX(nfts_cnt) AS max_cnt
        FROM CTE_net_buys
    ) AS cnt_limits
    CROSS JOIN UNNEST(SEQUENCE(cnt_limits.min_cnt, cnt_limits.max_cnt, 1)) AS T(CNT_COLUMN)
),
CTE_frame_summary AS (
    SELECT
        F.nfts_cnt,
        COALESCE(S.address_cnt, 0) AS address_cnt,
        SUM(COALESCE(F.nfts_cnt * COALESCE(S.address_cnt, 0), 0)) OVER (
            ORDER BY F.nfts_cnt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_nfts_cnt,
        SUM(COALESCE(S.address_cnt, 0)) OVER (
            ORDER BY F.nfts_cnt
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_address_cnt
    FROM CTE_frame F
    LEFT JOIN CTE_summary S
        ON F.nfts_cnt = S.nfts_cnt
)
-- 완성
SELECT
    nfts_cnt,
    address_cnt,
    cum_nfts_cnt,
    cum_address_cnt,
    CAST(cum_nfts_cnt AS DOUBLE) / SUM(nfts_cnt * address_cnt) OVER () AS cum_nfts_share,
    CAST(cum_address_cnt AS DOUBLE) / SUM(address_cnt) OVER () AS cum_address_share
FROM CTE_frame_summary
ORDER BY
    nfts_cnt
;