WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (
    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to",
        CASE
            WHEN contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 THEN 'Milady Maker CA'
            WHEN contract_address = 0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB THEN 'Redacted Remilio Babies CA'
            WHEN contract_address = 0x8a45fb65311ac8434aad5b8a93d1eba6ac4e813b THEN 'Milady, That B.I.T.C.H.'
        END AS contract_name,
        tokenId
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address IN (
            0x5Af0D9827E0c53E4799BB226655A1de152A425a5, -- Milady Maker CA
            0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB, -- Redacted Remilio Babies CA
            0x8a45fb65311ac8434aad5b8a93d1eba6ac4e813b -- Milady, That B.I.T.C.H.
        )
),
-- 일자별로 to_address의 NFT 매수 개수 & from_address의 NFT 매도 개수 구하기
CTE_buys_sells AS (
    SELECT
        evt_block_date,
        CAST("to" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady Maker CA') AS transfer_cnt_milady,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Redacted Remilio Babies CA') AS transfer_cnt_remilio,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady, That B.I.T.C.H.') AS transfer_cnt_bitch
    FROM CTE_raw
    GROUP BY
        evt_block_date, "to"
    UNION ALL
    SELECT
        evt_block_date,
        CAST("from" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady Maker CA') * -1 AS transfer_cnt_milady,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Redacted Remilio Babies CA') * -1 AS transfer_cnt_remilio,
        COUNT(evt_tx_hash) FILTER (WHERE contract_name = 'Milady, That B.I.T.C.H.') * -1 AS transfer_cnt_bitch
    FROM CTE_raw
    GROUP BY
        evt_block_date, "from"
),
-- 각 address의 NFT 보유 개수 구하기 (매수 개수 - 매도 개수)
CTE_net_buys AS (
    SELECT
        -- Etherscan Link of Each Address
        CONCAT(
            '<a href="https://etherscan.io/address/',
            CAST(address AS VARCHAR),
            '" target="_blank">',
            CAST(address AS VARCHAR),
            '</a>'
        ) AS address,
        SUM(transfer_cnt_milady) AS milady_cnt,
        SUM(transfer_cnt_remilio) AS remilio_cnt,
        SUM(transfer_cnt_bitch) AS bitch_cnt
    FROM CTE_buys_sells
    WHERE address NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x000000000000000000000000000000000000dead'
    )
    GROUP BY
        address
)
-- 완성
SELECT *
FROM CTE_net_buys
WHERE
    milady_cnt > 0 OR remilio_cnt > 0 OR bitch_cnt > 0
ORDER BY
    milady_cnt DESC, remilio_cnt DESC, bitch_cnt DESC
;