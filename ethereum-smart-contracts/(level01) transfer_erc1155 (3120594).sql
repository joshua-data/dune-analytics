WITH
-- (1) 본 컨트랙트 계정이 from이거나 to인 모든 ERC-1155 TransferSingle 리스트 가져오기
CTE_transfer_single_erc1155 AS (
    SELECT
        evt_block_time AS datetime,
        'single' AS transfer_type,
        evt_tx_hash AS tx_hash,
        "from" AS from_address,
        to AS to_address,
        contract_address,
        id,
        value
    FROM
        erc1155_ethereum.evt_TransferSingle
    WHERE
        evt_tx_hash IS NOT NULL
        AND "from" IS NOT NULL
        AND to IS NOT NULL
        AND (
            "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
            OR
            to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        )
),
-- (2) 본 컨트랙트 계정이 from이거나 to인 모든 ERC-1155 TransferBatch 리스트 가져오기
CTE_transfer_batch_erc1155 AS (
    SELECT
        MAIN.evt_block_time AS datetime,
        'batch' AS transfer_type,
        MAIN.evt_tx_hash AS tx_hash,
        MAIN."from" AS from_address,
        MAIN.to AS to_address,
        MAIN.contract_address,
        ids.id,
        "values".value
    FROM
        erc1155_ethereum.evt_TransferBatch MAIN
    CROSS JOIN
        UNNEST (MAIN.ids) AS ids(id)
    CROSS JOIN
        UNNEST (MAIN."values") AS "values"(value)
    WHERE
        MAIN.evt_tx_hash IS NOT NULL
        AND MAIN."from" IS NOT NULL
        AND MAIN.to IS NOT NULL
        AND (
            MAIN."from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
            OR
            MAIN.to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        )        
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
),
-- (1+2) TransferSingle + TransferBatch 리스트 합치기
CTE_transfer_erc1155 AS (
    SELECT *
    FROM CTE_transfer_single_erc1155
    UNION ALL
    SELECT *
    FROM CTE_transfer_batch_erc1155
)
SELECT
    *
FROM
    CTE_transfer_erc1155
;