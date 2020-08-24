/* 
   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   SPDX-License-Identifier: MIT-0
*/


/*
    Find top 100 customers with the most number of orders
*/

{{ config(materialized='table') }}

select c_custkey, c_nationkey, count(*) total_order
from customer c left join orders o on c.c_custkey = o.o_custkey
group by c_custkey, c_nationkey
order by total_order desc
limit 100
