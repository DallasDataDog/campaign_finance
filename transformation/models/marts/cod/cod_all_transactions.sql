SELECT 
    ID,
    RECORD_ID,
    REPORT_ID,
    FILE_LINK,
    FULL_NAME,
    CASE
        WHEN CONTACT_TYPE = 'Contributor' THEN 'Contribution'
        WHEN CONTACT_TYPE = 'Expenditure' THEN 'Expense'
        WHEN CONTACT_TYPE = 'Lender' THEN 'Loan'
        ELSE CONTACT_TYPE
    END AS TRANSACTION_TYPE,
    RECORD_TYPE,
    AMOUNT,
    SCHEDULE_TYPE,
    CANDIDATE_NAME,
    ELECTION_DATE,
    TRANSACTION_DATE,
    FULL_ADDRESS,
    STREET,
    CITY,
    STATE,
    ZIPCODE,
    GEO_LOCATION_LATITUDE,
    GEO_LOCATION_LONGITUDE
FROM {{ref("int_cod_dedup_all_report_records")}}