/* 
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: MIT-0
*/


/*
    Find 3 nations with the most active customers
*/

{{ config(materialized='table') }}

select n_name, count(*) as active_buyers
from nation n left join {{ ref('top_customers') }} c on n.n_nationkey = c.c_nationkey
group by n_name
order by active_buyers desc
limit 3