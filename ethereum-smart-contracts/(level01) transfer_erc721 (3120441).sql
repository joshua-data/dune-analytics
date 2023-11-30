datetime,
ca_address,
transfer_type,
tx_hash,
counter_address,
token_ca,
tokenId

WITH
-- (1) 본 컨트랙트 계정이 from이거나 to인 모든 ERC-721 Transfer 리스트 가져오기
CTE_transfer_erc721 AS (
    SELECT
        evt_block_time AS datetime,
        evt_tx_hash AS tx_hash,
        "from" AS from_address,
        to AS to_address,
        contract_address,
        tokenId
    FROM
        erc721_ethereum.evt_Transfer
    WHERE
        evt_tx_hash IS NOT NULL
        AND "from" IS NOT NULL
        AND to IS NOT NULL
        AND (
            "from" = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
            OR
            to = 0x00859b3baaC525143BB8A3ee3e19DDf9Daf2408c
        )
)
SELECT
    *
FROM
    CTE_transfer_erc721
;