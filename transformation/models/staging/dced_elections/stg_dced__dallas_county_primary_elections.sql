with CANDIDATES AS (
    SELECT *
    FROM {{source("DCED_ELECTIONS", 'DALLAS_COUNTY_PRIMARY_ELECTIONS')}}
)
SELECT
    TEC_FILER_IDENT,
    UPPER(TRIM(CONTEST_NAME)) AS CONTEST_NAME,
    UPPER(TRIM(CHOICE_NAME))  AS CHOICE_NAME,
    PARTY_NAME,
    TOTAL_VOTES,
    PERCENT_OF_VOTES,
    REGISTERED_VOTERS,
    BALLOTS_CAST,
    NUM_PRECINCT_TOTAL,
    NUM_PRECINCT_RPTG,
    OVER_VOTES,
    UNDER_VOTES
FROM CANDIDATES