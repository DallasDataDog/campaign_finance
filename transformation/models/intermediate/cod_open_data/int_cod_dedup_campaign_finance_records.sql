 WITH ALL_REPORT_RECORDS AS (
    SELECT 
        *,
        MD5(CONCAT(party_name, contact_type, amount, candidate_name, transaction_date, street)) AS RECORD_HASH,
        ROW_NUMBER() OVER(PARTITION BY RECORD_HASH ORDER BY SOURCE_FILE_NAME DESC) as ROW_NUM
    FROM {{ref("stg_cod__campaign_finance_records")}}
 )
    SELECT *
    FROM ALL_REPORT_RECORDS
    WHERE ROW_NUM = 1