
CREATE OR REPLACE TABLE RAW.COD_OPEN_DATA.CAMPAIGN_FINANCE_RECORDS (
    ID                       STRING       COMMENT 'Unique identifier for record',
    RECORD_ID                STRING       COMMENT 'Unique identifier for search',
    REPORT_ID                STRING       COMMENT 'Unique identifier for report',
    FILE_LINK                STRING       COMMENT 'PDF Link to Report',
    FIRST_NAME               STRING       COMMENT 'Client first name who paid the amount',
    LAST_NAME                STRING       COMMENT 'Client last name who paid the amount',
    BUSINESS_NAME            STRING       COMMENT 'Client Business name who paid the amount',
    CONTACT_TYPE             STRING       COMMENT 'Type of the client',
    RECORD_TYPE              STRING       COMMENT 'The type of the contribution',
    AMOUNT                   FLOAT        COMMENT 'The amount of the contribution',
    SCHEDULE_TYPE            STRING,
    CANDIDATE_NAME           STRING,
    ELECTION_DATE            DATE,
    TRANSACTION_DATE         DATE,
    STREET                   STRING,
    CITY                     STRING,
    STATE                    STRING,
    ZIPCODE                  STRING,
    GEO_LOCATION_LATITUDE    FLOAT,
    GEO_LOCATION_LONGITUDE   FLOAT,
    CREATED_AT               TIMESTAMP,
    UPDATED_AT               TIMESTAMP,
    SOURCE_FILE_NAME         STRING,
    LOAD_DATE                DATE
);

COPY INTO RAW.COD_OPEN_DATA.CAMPAIGN_FINANCE_RECORDS
FROM
    (
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        CASE WHEN $13 = '' THEN DATE('9999-12-31') ELSE DATE($13) END,
        CASE WHEN $14 = '' THEN DATE('9999-12-31') ELSE DATE($14) END,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        METADATA$FILENAME,
        DATE(CURRENT_TIMESTAMP())
        FROM @RAW.COD_OPEN_DATA.RAW_FILES
    )
FILE_FORMAT = RAW.COD_OPEN_DATA.CAMPAIGN_FINANCE_CSV_FORMAT
PATTERN = 'dallas_campaign_finance_.*[.]csv[.]gz'
;