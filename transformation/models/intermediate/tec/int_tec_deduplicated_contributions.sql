with
    contributions as (
        select
            *,
            md5(
                concat(
                    filer_ident,
                    '|',
                    coalesce(contribution_dt, '9999-12-31'),
                    '|',
                    contribution_amount,
                    '|',
                    coalesce(contributor_full_name, ''),
                    coalesce(contributor_street_postal_code, '')
                )
            ) record_hash
        from {{ ref("stg_tec__contributions") }}
        WHERE UPPER(INFO_ONLY_FLAG) = 'N'
    ),
    row_numbered as (
        select
            *,
            row_number() over (partition by record_hash order by received_dt desc) as row_num
        from contributions
    )
select *
from row_numbered
where row_num = 1
