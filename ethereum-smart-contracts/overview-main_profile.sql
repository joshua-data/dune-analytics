WITH
CTE_erc20_transfers_cnt AS (
    SELECT
        COUNT(*) AS transfers_cnt
    FROM
        erc20_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
),
CTE_erc721_transfers_cnt AS (
    SELECT
        COUNT(*) AS transfers_cnt
    FROM
        erc721_ethereum.evt_Transfer
    WHERE
        contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44
),
CTE_erc1155_transfers_cnt AS (
    SELECT
        SUM(transfers_cnt) AS transfers_cnt
    FROM (
        SELECT COUNT(*) AS transfers_cnt
        FROM erc1155_ethereum.evt_TransferSingle
        WHERE contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44    
        UNION ALL
        SELECT COUNT(*) AS transfers_cnt
        FROM erc1155_ethereum.evt_TransferBatch
        WHERE contract_address = 0x5283D291DBCF85356A21bA090E6db59121208b44    
    )
),
CTE_profile AS (
    SELECT
        name AS project,
        namespace AS contract_name,
        CASE
            WHEN (SELECT transfers_cnt FROM CTE_erc20_transfers_cnt) > 0 THEN 'ERC-20'
            WHEN (SELECT transfers_cnt FROM CTE_erc721_transfers_cnt) > 0 THEN 'ERC-721'
            WHEN (SELECT transfers_cnt FROM CTE_erc1155_transfers_cnt) > 0 THEN 'ERC-1155'
            ELSE 'Unknown'
        END AS contract_type,
        "from" AS deployer
    FROM
        ethereum.contracts
    WHERE
        address = 0x5283D291DBCF85356A21bA090E6db59121208b44
)
SELECT
    *
FROM
    CTE_profile
LIMIT
    1
;