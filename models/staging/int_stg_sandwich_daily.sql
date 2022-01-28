with stg_daily_san as (select branch,shift,
date,year,sum(total_seconds) as total_time,
avg(person_sandwicharea) as person ,CASE WHEN shift='PM' then 5 
     WHEN shift='WDAM' then 4
     WHEN shift='BRUNCH' then 2
     WHEN shift='WEAM' then 3
     ELSE 0 END AS av_time
 from {{ ref('int_stg_sandwichtime') }} group by year,date,
branch,shift),
daily as (select round(total_time/(av_time*3600*person),2)as sandwich_manned,year ,date,shift,
branch from stg_daily_san where shift!='OTHERS'

)
select * from daily
