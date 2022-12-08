{{ config(materialized='table')}}
-- view, will be updated automatically, but potentially slower
-- table, will not be updated automatically, but potentially faster

with votes as (
  select date_trunc('day', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
  user_id,
  count(*) as contributions,
  'votes' as type
  from producthunt_db.votes 
  where created_at >= '2021-01-01'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  group by 1,2
)
, posts as (
select date_trunc('day', ((scheduled_at + interval '-7 hour'))::timestamp)::date as date, 
    user_id,
    count(*) as contributions,
    'posts' as type
    from producthunt_db.posts 
    where trashed_at is null 
    and scheduled_at >= '2021-01-01'
    and user_id NOT IN (select id from producthunt_db.users where role IN (100))
    and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
    and trashed_at is null
    group by 1,2
)
, discussions as (
    select date_trunc('day', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
    user_id,
    count(*) as contributions,
    'discussions' as type
    from producthunt_db.discussion_threads 
    where trashed_at is null 
    and created_at >= '2021-01-01'
    and user_id NOT IN (select id from producthunt_db.users where role IN (100))
    and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
    and status not in ('rejected', 'pending')
    and trashed_at is null
    group by 1,2
)

, comments as (
  select date_trunc('day', ((created_at + interval '-7 hour'))::timestamp)::date as date,  
  user_id,
  count(*) as contributions,
  'comments' as type
  from producthunt_db.comments 
  where created_at >= '2021-01-01'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  and trashed_at is null
  group by 1,2
)
, reviews as (
  select date_trunc('day', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
  user_id,
  count(*) as contributions,
  'reviews' as type
  from producthunt_db.reviews
  where created_at >= '2021-01-01'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  group by 1,2
)
select {{ dbt_utils.surrogate_key(
      'date',
      'user_id',
      'type'
  ) }} as contribution_id,
  *
from (
  select *
  from votes 
  union all
  select *
  from posts
  union all
  select *
  from discussions
  union all
  select *
  from reviews
) x 
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
