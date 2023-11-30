WITH
-- evt_Transfer 테이블 준비
CTE_raw AS (
    SELECT
        CAST(evt_block_time AS DATE) AS evt_block_date,
        evt_tx_hash,
        "from", "to"
    FROM erc721_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5Af0D9827E0c53E4799BB226655A1de152A425a5 -- Milady Maker CA
),
-- 일자별로 to_address의 NFT 매수 개수 & from_address의 NFT 매도 개수 구하기
CTE_buys_sells AS (
    SELECT
        evt_block_date,
        CAST("to" AS VARCHAR) AS address,
        COUNT(evt_tx_hash) * 1 AS transfer_cnt
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
-- 일자별로 각 address의 NFT 순매수 개수 구하기 (매수 개수 - 매도 개수)
CTE_net_buys AS (
    SELECT
        evt_block_date,
        address,
        SUM(transfer_cnt) AS net_buys
    FROM CTE_buys_sells
    GROUP BY
        evt_block_date, address
),
-- 오늘 기준으로 각 address의 NFT 현재 보유 개수 구하기
CTE_today_balance AS (
    SELECT
        address,
        SUM(COALESCE(net_buys, 0)) AS balance
    FROM CTE_net_buys
    GROUP BY
        address
),
-- NFT 1개 이상 보유한 address만 표시하기
CTE_holder_address AS (
    SELECT
        address
    FROM CTE_today_balance
    WHERE
        balance > 0
        AND address NOT IN (
            '0x0000000000000000000000000000000000000000',
            '0x000000000000000000000000000000000000dead'
        )
),

CTE_token_in AS (
    SELECT
        CAST("to" AS VARCHAR) AS address,
        CAST(contract_address AS VARCHAR) AS contract_address,
        COUNT(evt_tx_hash) * 1 AS transfer_cnt
    FROM erc721_ethereum.evt_Transfer
    WHERE
        CAST("to" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND contract_address NOT IN (
            0x5Af0D9827E0c53E4799BB226655A1de152A425a5, -- Milady Maker CA
            0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB, -- Remilio
            0x8A45Fb65311aC8434AaD5b8a93D1EbA6Ac4e813b -- Milady The B.I.T.C.H.            
        )
        AND contract_address IS NOT NULL
    GROUP BY
        CAST("to" AS VARCHAR), CAST(contract_address AS VARCHAR)
),
CTE_token_out AS (
    SELECT
        CAST("from" AS VARCHAR) AS address,
        CAST(contract_address AS VARCHAR) AS contract_address,
        COUNT(evt_tx_hash) * -1 AS transfer_cnt
    FROM erc721_ethereum.evt_Transfer
    WHERE
        CAST("from" AS VARCHAR) IN (SELECT address FROM CTE_holder_address)
        AND contract_address NOT IN (
            0x5Af0D9827E0c53E4799BB226655A1de152A425a5, -- Milady Maker CA
            0xD3D9ddd0CF0A5F0BFB8f7fcEAe075DF687eAEBaB, -- Remilio
            0x8A45Fb65311aC8434AaD5b8a93D1EbA6Ac4e813b -- Milady The B.I.T.C.H.            
        )
        AND contract_address IS NOT NULL
    GROUP BY
        CAST("from" AS VARCHAR), CAST(contract_address AS VARCHAR)    
),
CTE_token_in_out AS (
    SELECT
        TOKEN_IN.address,
        TOKEN_IN.contract_address,
        COALESCE(TOKEN_IN.transfer_cnt, 0) - COALESCE(TOKEN_OUT.transfer_cnt, 0) AS token_balance
    FROM CTE_token_in TOKEN_IN
    LEFT JOIN CTE_token_out TOKEN_OUT
        ON TOKEN_IN.address = TOKEN_OUT.address
            AND TOKEN_IN.contract_address = TOKEN_OUT.contract_address
),
CTE_summary AS (
    SELECT
        CONCAT(
            '<a href="https://etherscan.io/address/',
            A.address,
            '" target="_blank">',
            'Click</a>'            
        ) AS etherscan_link,
        A.address,
        INOUT.contract_address,
        INOUT.token_balance
    FROM CTE_holder_address A
    LEFT JOIN CTE_token_in_out INOUT ON A.address = INOUT.address
),
CTE_summary_ca_labels AS (
    SELECT
        MAIN.contract_address,
        LABELS.name,
        COUNT(DISTINCT MAIN.address) FILTER (WHERE MAIN.token_balance > 0) AS address_cnt
    FROM CTE_summary MAIN
    LEFT JOIN tokens.nft LABELS
        ON LOWER(MAIN.contract_address) = LOWER(CAST(LABELS.contract_address AS VARCHAR))
    WHERE
        LABELS.blockchain = 'ethereum'
    GROUP BY
        MAIN.contract_address, LABELS.name
)
SELECT
    CONCAT(
        '<a href="https://etherscan.io/address/',
        contract_address,
        '" target="_blank">',
        'Click</a>'            
    ) AS etherscan_link,
    name,
    contract_address,
    address_cnt,
    CAST(address_cnt AS DOUBLE) / (SELECT COUNT(DISTINCT address) FROM CTE_holder_address) AS address_share
FROM CTE_summary_ca_labels
ORDER BY
    address_cnt DESC
;