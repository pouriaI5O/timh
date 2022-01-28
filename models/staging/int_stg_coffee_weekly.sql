with stg_weekly as(select branch,shift,
EXTRACT(WEEK FROM date) as week,year,sum(total_seconds) as total_time,
avg(person_coffeearea) as person,
CASE WHEN shift='PM' then 35 
     WHEN shift='WDAM' then 20
     WHEN shift='BRUNCH' then 14
     WHEN shift='WEAM' then 6
     ELSE 0 END AS av_time from {{ ref('int_stg_timcoffee') }} group by year,week,
branch,shift),

stg_coffee_weekly_final as(select round(total_time/(av_time*3600*person),2)as coffee_manned,year as year_c,week as week_c,shift as shift_c,branch as branch_c from stg_weekly where shift!='OTHERS')

select * from stg_coffee_weekly_final
