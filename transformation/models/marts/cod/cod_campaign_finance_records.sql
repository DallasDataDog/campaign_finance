SELECT 
    ID,
    RECORD_ID,
    REPORT_ID,
    FILE_LINK,
    FULL_NAME,
    CASE
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'CAROLYN KING ARNOLD' AND RECORD_TYPE = 'January 15: Semi-Annual 2023' AND TRANSACTION_DATE < '2024-07-01' THEN 'Contribution' --This an anomaly found in Carolyn King's records that report in-kind contributions as "Report Self"

        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'ALBERT MATA' THEN 'Contribution'                                                                                   --All of the records in the dataset are listed as "Report Itself" are actually in-kind contributions

        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'CARA MENDELSOHN' AND TRANSACTION_DATE < '2024-07-01' AND RECORD_ID NOT IN ('79324','87703') THEN 'Expense'         --Most of the records in the dataset are listed as "Report Itself" are actually expenses paid for by credit card. This labels them properly as expenses
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'CARA MENDELSOHN' AND TRANSACTION_DATE < '2024-07-01' AND RECORD_ID     IN ('79324','87703') THEN 'Contribution'    --In Kind contributions

        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'JUDITH KUMAR' AND RECORD_ID <> '68499' THEN 'Expense'                                                              --Most of the records in the dataset from Judith Kumar listed as "Report Itself" are actually expenses paid for by credit card. This labels them properly as expenses
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'JUDITH KUMAR' AND RECORD_ID =  '68499'  THEN 'Contribution'                                                        --This was an in-kind donation, the data incorrectly labled as "Report Itself"

        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'AMANDA SCHULZ' AND RECORD_ID <> '82519' AND TRANSACTION_DATE < '2024-07-01' THEN 'Expense'                         --Most of the records in the dataset listed as "Report Itself" are actually expenses paid for by credit card. This labels them properly as expenses
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'AMANDA SCHULZ' AND RECORD_ID =  '82519'  AND TRANSACTION_DATE < '2024-07-01' THEN 'Contribution'                   --This was an in-kind donation, the data incorrectly labled as "Report Itself"

        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'GAY WILLIS' AND TRANSACTION_DATE < '2024-07-01' AND RECORD_ID NOT IN ('70878','74828') THEN 'Expense' --Most of the records in the dataset are listed as "Report Itself" are actually expenses paid for by credit card. This labels them properly as expenses
        WHEN SCHEDULE_TYPE ='Report Itself' AND UPPER(CANDIDATE_NAME) = 'GAY WILLIS' AND TRANSACTION_DATE < '2024-07-01' AND RECORD_ID     IN ('70878','74828') THEN 'Contribution'  --In kind and regular donations

        WHEN CONTACT_TYPE = 'Contributor' THEN 'Contribution'
        WHEN CONTACT_TYPE = 'Expenditure' THEN 'Expense'
        WHEN CONTACT_TYPE = 'Lender' THEN 'Loan'
        ELSE CONTACT_TYPE
    END AS TRANSACTION_TYPE,
    RECORD_TYPE,
    CASE
        WHEN FULL_NAME = 'MASTER CARD - CITI' AND CANDIDATE_NAME = 'CARA MENDELSOHN' THEN 0 --Cara Mendelsohn often pays her expenses with a credit card, those expenses should be listed as Expenditures made by Credit Card, so we need to exclude these charges as to not double count. I am making then 0.
        WHEN FULL_NAME = 'CAPITAL ONE CREDIT CARDS' AND CANDIDATE_NAME = 'JUDITH KUMAR' THEN 0 
        ELSE AMOUNT
    END AS AMOUNT,
    SCHEDULE_TYPE,
    CANDIDATE_NAME,
    ELECTION_DATE,
    TRANSACTION_DATE,
    CASE
        WHEN FULL_NAME IN (
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
            'LELAND BURK',
            'LOU OLERIO',
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
    GEO_LOCATION_LATITUDE,
    GEO_LOCATION_LONGITUDE
FROM {{ref("int_cod_dedup_campaign_finance_records")}}