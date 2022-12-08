with t1 as (
    select 
    date, count(visitor_id) as users 
    from {{ ref('dau') }}
    group by 1
)
select *
from t1 
