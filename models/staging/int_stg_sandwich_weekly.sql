with stg_weekly as(select branch,shift,
EXTRACT(WEEK FROM date) as week,year,sum(total_seconds) as total_time,
avg(person_sandwicharea) as person,
CASE WHEN shift='PM' then 35 
     WHEN shift='WDAM' then 20
     WHEN shift='BRUNCH' then 14
     WHEN shift='WEAM' then 6
     ELSE 0 END AS av_time from {{ ref('int_stg_sandwichtime') }} group by year,week,
branch,shift),

stg_san_weekly_final as(select round(total_time/(av_time*3600*person),2)as sandwich_manned,year,week,shift,branch from stg_weekly where shift!='OTHERS')

select * from stg_san_weekly_final