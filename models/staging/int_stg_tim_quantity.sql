{{ config(
    materialized='table'
)}}

with stg_talbot as(SELECT  *,
        ISNULL(date, (SELECT TOP 1 date from {{ source('gh','talbot_255') }} WHERE _row < t._row AND date IS NOT NULL ORDER BY _row DESC))
  from {{ source('gh','talbot_255') }}  t),
  int_stg_talbot as (
  select type,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,coalesce as date,1642 as branch_id,'TH-ON-Leamington-1642-255-Talbot'as branch  from stg_talbot order by date ),
  talbot_255 as (
  select * from int_stg_talbot where type is not null order by date),
  
  
  stg_maidstone as(SELECT  *,
        ISNULL(date, (SELECT TOP 1 date from {{ source('gh','maidston_1903') }} WHERE _row < t._row AND date IS NOT NULL ORDER BY _row DESC))
  from {{ source('gh','maidston_1903') }}  t),
  int_stg_maidstone as (
  select type,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,coalesce as date,1903 as branch_id, 'TH-ON-Essex-1903-9-Maidstone'as branch  from stg_maidstone order by date ),
  maidstone_1903 as (
  select * from int_stg_maidstone where type is not null order by date),


stg_erie185 as(SELECT  *,
        ISNULL(date, (SELECT TOP 1 date from {{ source('gh','erie_185') }} WHERE _row < t._row AND date IS NOT NULL ORDER BY _row DESC))
  from {{ source('gh','erie_185') }}  t),
  int_stg_erie185 as (
  select type,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,coalesce as date, 562 as branch_id,'TH-ON-Leamington-562-185-Erie'as branch  from stg_erie185 order by date ),
  erie185 as (
  select * from int_stg_erie185 where type is not null order by date),

  stg_erie250 as(SELECT  *,
        ISNULL(date, (SELECT TOP 1 date from {{ source('gh','erie_250') }} WHERE _row < t._row AND date IS NOT NULL ORDER BY _row DESC))
  from {{ source('gh','erie_250') }}  t),
  int_stg_erie250 as (
  select type,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,coalesce as date,324 as branch_id, 'TH-ON-Leamington-324-250-Erie'as branch  from stg_erie250 order by date ),
  erie250 as (
  select * from int_stg_erie250 where type is not null order by date),

  stg_union as (
        select * from talbot_255 
        UNION
        select *from maidstone_1903
        UNION
        select * from erie185
        UNION
        select * from erie250
  ),
  stg_av_time as (
  select upper(type) as shift ,branch_id,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,date,branch,
CASE WHEN shift='PM' then 35 
     WHEN shift='WDAM' then 20
     WHEN shift='BRUNCH' then 14
     WHEN shift='WEAM' then 6
     ELSE 0 END AS av_time
   from stg_union order by date desc),

   score as(select  branch_id,shift ,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,av_time,CONVERT(datetime, date),branch,
  case when drive_thru_time<=22.5 and shift='WDAM' then 18
        when drive_thru_time>22.51 and drive_thru_time<25 and shift='WDAM' then 16
        when drive_thru_time>25.01 and drive_thru_time<27.5 and shift='WDAM' then 14
        when drive_thru_time>27.51 and drive_thru_time<30 and shift='WDAM' then 10
        when drive_thru_time>30.01 and drive_thru_time<32.5 and shift='WDAM' then 6
        when drive_thru_time>32.51 and drive_thru_time<35 and shift='WDAM' then 3
        when drive_thru_time>35 and shift='WDAM' then 0
        when drive_thru_time<=27.5 and shift='WEAM' then 8
        when drive_thru_time>27.51  and drive_thru_time<30 and shift='WEAM' then 7
        when drive_thru_time>30.01 and drive_thru_time<32.51 and shift='WEAM' then 6
        when drive_thru_time>32.51 and drive_thru_time<35 and shift='WEAM' then 4
        when drive_thru_time>35.01 and drive_thru_time<37.5 and shift='WEAM' then 3
        when drive_thru_time>37.51 and drive_thru_time<40 and shift='WEAM' then 2
        when drive_thru_time>40 and shift='WEAM' then 0
        when drive_thru_time<=32.5 and shift='BRUNCH' then 10
        when drive_thru_time>32.51 and drive_thru_time<35 and shift='BRUNCH' then 9
        when drive_thru_time>35.01 and drive_thru_time<37.5 and shift='BRUNCH' then 8
        when drive_thru_time>37.51 and drive_thru_time<40 and shift='BRUNCH' then 4
        when drive_thru_time>40.01 and drive_thru_time<42.5 and shift='BRUNCH' then 3
        when drive_thru_time>42.51 and drive_thru_time<45 and shift='BRUNCH' then 2
        when drive_thru_time>45 and shift='BRUNCH' then 0
        when drive_thru_time<=37.5 and shift='PM' then 14
        when drive_thru_time>37.51 and drive_thru_time<40 and shift='PM' then 13
        when drive_thru_time>40.01 and drive_thru_time<42.5 and shift='PM' then 12
        when drive_thru_time>42.51 and drive_thru_time<45 and shift='PM' then 8
        when drive_thru_time>45.01 and drive_thru_time<47.5 and shift='PM' then 4
        when drive_thru_time>47.51 and drive_thru_time<50 and shift='PM' then 3
        when drive_thru_time>50 and shift='PM' then 0
        ELSE -1 END AS customer_score  from stg_av_time),

   sale as(select branch_id,shift ,sandwich_sales_quantity,coffee_sales_quantity
  ,drive_thru_time,av_time,date,EXTRACT(WEEK FROM date) as week ,branch,customer_score,EXTRACT(YEAR FROM date) AS year, 
    round(coffee_sales_quantity/av_time,2) as coffee_sale_per_hour,
    round(sandwich_sales_quantity/av_time,2) as sandwich_sale_per_hour from score where customer_score>-1 )

select * from sale

  


  


   
   
  