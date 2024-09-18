SELECT 
    ID,
    RECORD_ID,
    REPORT_ID,
    FILE_LINK,
    PARTY_NAME,
    PARTY_TYPE,
    CASE
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'CAROLYN KING ARNOLD' AND RECORD_TYPE = 'January 15: Semi-Annual 2023'  THEN 'Contribution'        --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'ALBERT MATA' THEN 'Contribution'                                                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'OMAR NARVAEZ'     AND RECORD_ID IN ('84998') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'JUDITH KUMAR'     AND RECORD_ID IN ('68499') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'AMANDA SCHULZ'    AND RECORD_ID IN ('82519') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'FELIX GRIGGS'     AND RECORD_ID IN ('83047') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'JOHNNY AGUINAGA'  AND RECORD_ID IN ('69807') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'MICHAEL FETZER'   AND RECORD_ID IN ('71802') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'JAIME RESENDEZ'   AND RECORD_ID IN ('88004') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'RAHA ASSADI'      AND RECORD_ID IN ('69080') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'STEPHANI KYLE'    AND RECORD_ID IN ('71820') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'TRACY DOTIE-HILL' AND RECORD_ID IN ('83927') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'YOLANDA WILLIAMS' AND RECORD_ID IN ('87733') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'ZARIN GRACEY'     AND RECORD_ID IN ('83840') THEN 'Contribution'                                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'ISRAEL VARELA'    AND RECORD_ID IN ('67234','73587') THEN 'Contribution'                          --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'GAY WILLIS'       AND RECORD_ID IN ('70878','74828') THEN 'Contribution'                          --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'CARA MENDELSOHN'  AND RECORD_ID IN ('79324','87703') THEN 'Contribution'                          --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'PAUL RIDLEY'      AND RECORD_ID IN ('76399','76760','84612','84226','87317') THEN 'Contribution'  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'LELAND BURK'      AND RECORD_ID IN ('73593','73347','73351') THEN 'Contribution'                  --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'PAULA BLACKMON'   AND RECORD_ID IN ('72813','72816','72814','72818') THEN 'Contribution'          --Correcting this record as it was an in-kind donation, that was incorrectly labled as "Report Itself"
        WHEN CONTACT_TYPE  ='Contributor' THEN 'Contribution'
        WHEN CONTACT_TYPE  ='Expenditure' THEN 'Expense'
        WHEN CONTACT_TYPE  ='Lender' THEN 'Loan'
        WHEN SCHEDULE_TYPE ='Report Itself' THEN 'Not Classified'
        ELSE CONTACT_TYPE
    END AS TRANSACTION_TYPE,
    RECORD_TYPE,
    AMOUNT,
    SCHEDULE_TYPE,
    CANDIDATE_NAME,
    ELECTION_DATE,
    TRANSACTION_DATE,
    DATE_TRUNC('MONTH', DATE(TRANSACTION_DATE)) AS TRANSACTION_MONTH, 
    CASE
        WHEN PARTY_NAME IN (
            'ADAM MURPHY',
            'APARTMENT ASSOCIATION OF GREATER DALLAS PAC',
            'BRADLEY JOHNSON',
            'BRADY WOOD',
            'BRIAN ALEF',
            'CHARLES CORSON',
            'CHARLEY DORSANEO',
            'CHRIS KLEINERT',
            'CLAYTON LIKOVER',
            'COATS ROSE, P.C. PAC',
            'DANNY BAKER',
            'DAVID HELLER',
            'DON SILVERMAN',
            'ELENA LICARI',
            'ERIN GRANBERRY',
            'FRANK MIHALOPOULOS',
            'GRANT WOODRUFF',
            'HARLAN CROW',
            'HAROLD GINSBURG',
            'HBA OF GREATER DALLAS HOME PAC',
            'HUDSON HENLEY',
            'JAKE MILNER',
            'JAMES MCCORMICK',
            'JANE WEEMPE',
            'JEFF CAREY',
            'JEFFREY HOWARD',
            'JIM CHRISTON',
            'JOHN FARBES',
            'JOHN SNYDER',
            'JOSEPH DINGMAN',
            'JOSEPH PITCHFORD',
            'KATIE WURST',
            'KATY SLADE',
            'KEITH POMYKAL',
            'KIRK WILSON',
            'LARKSPUR CAPITAL LP',
            'LELAND BURK',
            'LOU OLERIO',
            'LOUIS OLERIO',
            'LOUIS M. OLERIO, JR.',
            'OLERIO HOMES',
            'LUCY BILLINGSLEY',
            'MARC ANDRES',
            'MATT SEGREST',
            'MATTHEW VRUGGINK',
            'MICHAEL FARRIS',
            'MIKE ABLON',
            'NICK CASSAVECHIA',
            'NICK VENGHAUS',
            'RICK PERDUE',
            'ROBERT LEE',
            'ROSS PEROT',
            'ROSS PEROT JR.',
            'RYAN CROW',
            'STEPHEN MILLER',
            'METROTEX ASSOCIATION OF REALTORS PAC',
            'THE REAL ESTATE COUNCIL PAC (TRECPAC)',
            'TEXAS ASSOCIATION OF REALTORS PAC (TREPAC)',
            'TIM BYRNE',
            'TODD PETTY',
            'WADE ANDRES',
            'WADE JOHNS',
            'WARREN ANDRES',
            'WILLIAM ROUSE') THEN 'REAL ESTATE'
        ELSE 'OTHER'
    END AS INDUSTRY_OF_CONTRIBUTOR,
    FULL_ADDRESS,
    STREET,
    CITY,
    STATE,
    ZIPCODE,
    CONCAT('(',GEO_LOCATION_LATITUDE,', ' ,GEO_LOCATION_LONGITUDE,')') as GEO_COORDINATES
FROM {{ref("int_cod_dedup_campaign_finance_records")}}
WHERE CANDIDATE_NAME != 'COUNCIL MEMBER'        --Filtering out these records as they appear to be some sort of test records
    AND CONTACT_TYPE != 'Candidate / Committee' --Filtering out records that track the submissions of reports