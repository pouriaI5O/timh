with coffee as ( select Branch,class,Count,Date,year,Hour,Minute,
Second  from {{ ref('stg_times') }} where class='person_coffeearea' and count>0),

coffee_dup as (select count(*) as total_second,Branch,
round(avg(Count),3) as person_coffeearea,Date,year,Hour,Minute,
Second from coffee group by branch,Date,year,Hour,Minute,Second),

coffee_time as ( select branch,person_coffeearea,Date,
date_part(weekday,date) as day,year,Hour,Minute,second,1 as total_seconds,total_second from coffee_dup),

coffee_shift as (select date,year, person_coffeearea,branch,
total_seconds,total_second,
case when day=0 then 7 else day end as dayofweek,
cast(CONCAT(Date,' ',CAST(hour AS VARCHAR(2)),':',CAST(minute AS VARCHAR(2))) as datetime)as Timestamps,
cast(Timestamps AS time) as Time,
           CASE WHEN time > '06:00:00' and time < '10:00:00' and dayofweek<6  then 'WDAM' 
                WHEN time > '10:00:00' and time < '12:00:00' then'BRUNCH'
                WHEN time > '12:00:00' and time < '17:00:00' then'PM'
                WHEN time > '07:00:00' and time < '10:00:00' and dayofweek>5 then 'WEAM' 
                 ELSE 'OTHERS' END AS shift

                 from coffee_time )


select*from coffee_shift
