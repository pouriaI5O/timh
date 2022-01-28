with stg_daily as (select branch,shift,year,
date,sum(total_seconds) as total_time,
avg(person_coffeearea) as person ,CASE WHEN shift='PM' then 5 
     WHEN shift='WDAM' then 4
     WHEN shift='BRUNCH' then 2
     WHEN shift='WEAM' then 3
     ELSE 0 END AS av_time
 from {{ ref('int_stg_timcoffee') }} group by year,date,
branch,shift),
daily as (select round(total_time/(av_time*3600*person),2)as coffee_manned,year as year_c,
date as date_c,shift as shift_c,
branch as branch_c from stg_daily where shift!='OTHERS'

)
select * from daily
