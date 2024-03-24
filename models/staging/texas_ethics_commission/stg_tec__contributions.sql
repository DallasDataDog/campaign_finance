
with CONTRIBUTIONS as (
    SELECT *
    FROM {{ref("base_tec__contributions")}}
),
CONTRIBUTIONS_PRE_ELECTION AS (
    SELECT *
    FROM {{ref("base_tec__contributions_pre_election")}}
)
SELECT * FROM CONTRIBUTIONS
UNION ALL
SELECT * FROM CONTRIBUTIONS_PRE_ELECTION