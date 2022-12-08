-- {{ config(materialized='table')}}

with web as (
  select date_trunc('day', ((received_at + interval '-8 hour'))::timestamp)::date as date, 
  coalesce(user_id, concat(context_ip, context_user_agent)) as visitor_id,
  case when user_id is not null then 1 else 0 end as logged_in,
  case when context_user_agent like '%Andriod%' or
  context_user_agent like '%iPhone%' or 
  context_user_agent like '%iPad%' then 'Mobile web' else 'Desktop web' end as platform,
  count(*) as pageviews
  from producthunt_production.pages 
  where received_at >= '2021-01-01' 
    and coalesce(user_id, '0') NOT IN (select id::varchar
                                  from producthunt_db.users 
                                  where role IN (3, 10))
    and context_ip NOT IN (select value
                    from producthunt_db.spam_filter_values
                    where filter_kind = 0)
    and context_user_agent NOT SIMILAR TO '%(Bot|bot)%'
    and path NOT IN (select concat('/discussions/', replace(slug, concat(id::varchar, '-'), ''))
                  from producthunt_db.discussion_threads
                  where status in ('rejected', 'pending'))
    group by 1,2,3,4
)
, mobile as (
    select date_trunc('day', ((received_at + interval '-8 hour'))::timestamp)::date as date,  
    COALESCE(user_id, anonymous_id) as visitor_id,
    case when user_id is not null then 1 else 0 end as logged_in,
    case when context_device_type = 'ios' then 'iOS' else 'Android' end as platform,
    count(*) as pageviews
    from producthunt_production.screens 
    where received_at >= '2021-01-01'  
    and coalesce(user_id, 'na') NOT IN (select id::varchar
                                  from producthunt_db.users 
                                  where role IN (3, 10))
    and context_ip NOT IN (select value
                    from producthunt_db.spam_filter_values
                    where filter_kind = 0)
    group by 1,2,3,4
  )
select {{dbt_utils.surrogate_key(
      'date',
      'visitor_id',
      'platform') }} as id,
      *
    -- date, platform,
    -- count(distinct visitor_id) as dau,
    -- sum(pageviews) as pageviews,
    -- sum(case when logged_in = 1 then pageviews end) as logged_in_pageviews,
    -- count(case when logged_in = 1 then visitor_id end) as logged_in_users
  from
  (
    select *
    from web
    union 
    select *
    from mobile
)