with fundraising_stats AS(
    SELECT 
        CANDIDATE_NAME,
        SUBSTR(TRANSACTION_DATE,0,7) AS TRANSACTION_MONTH,
        SUM(AMOUNT)         AS MONEY_RAISED,
        COUNT(*)            AS NUMBER_OF_CONTRIBUTIONS
    FROM {{ref("cod_all_transactions")}}
    WHERE 
    (TRANSACTION_DATE BETWEEN '2023-07-01' AND '2023-12-31')
    AND TRANSACTION_TYPE = 'Contribution'
    AND CANDIDATE_NAME = 'CHAD WEST'
    GROUP BY CANDIDATE_NAME, TRANSACTION_MONTH
    ORDER BY MONEY_RAISED DESC
)
SELECT
    A.CANDIDATE_NAME,
    A.TRANSACTION_MONTH,
    A.MONEY_RAISED,
    A.NUMBER_OF_CONTRIBUTIONS,
FROM fundraising_stats A
ORDER BY CANDIDATE_NAME ASC, TRANSACTION_MONTH ASC