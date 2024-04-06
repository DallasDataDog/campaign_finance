CREATE OR REPLACE TABLE DALLAS_CITY_COUNCIL_ELECTIONS (
    ELECTION            DATE,
    DISTRICT            STRING,
    CANDIDATE           STRING,
    INCUMBENT           STRING,
    VOTES               NUMBER,
    RESULT              STRING,
    SOURCE_FILE_NAME    STRING,
    LOAD_DATE           DATE
);

COPY INTO DALLAS_CITY_COUNCIL_ELECTIONS
FROM
    (
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        METADATA$FILENAME,
        DATE(CURRENT_TIMESTAMP())
        FROM @RAW_FILES/dallas_city_council_elections.csv.gz
    )
FILE_FORMAT = CSV_FILES;



CREATE OR REPLACE TABLE DALLAS_COUNTY_PRIMARY_ELECTIONS (
    ELECTION_DATE        DATE,
    TEC_FILER_IDENT      STRING,
    CONTEST_NAME         STRING,
    CHOICE_NAME          STRING,
    PARTY_NAME           STRING,
    TOTAL_VOTES          STRING,
    PERCENT_OF_VOTES     FLOAT,
    REGISTERED_VOTERS    NUMBER,
    BALLOTS_CAST         NUMBER,
    NUM_PRECINCT_TOTAL   NUMBER,
    NUM_PRECINCT_RPTG    NUMBER,
    OVER_VOTES           NUMBER,
    UNDER_VOTE           NUMBER,
    SOURCE_FILE_NAME     STRING,
    LOAD_DATE            DATE
);

COPY INTO DALLAS_COUNTY_PRIMARY_ELECTIONS
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
        $13,
        METADATA$FILENAME,
        DATE(CURRENT_TIMESTAMP())
        FROM @RAW_FILES/dallas_county_primary_elections.csv.gz
    )
FILE_FORMAT = CSV_FILES;



