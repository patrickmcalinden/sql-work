--removes all of the null data assuming that no row can have a blank start station with other valid columns of data \\saving result of this as view in bigquery
--turned this result into view, seen below on next query
select *
from bigquery-public-data.new_york_citibike.citibike_trips
where start_station_id is not Null;

--what gender takes more trips (no way to see repeat customers)/acessing view for query
select gender, Count(gender) as gender_count
from `citibike_trips_modified.trips`
group by gender
order by gender_count desc;
 
--what is the average trip duration(in minutes) by gender
select gender, CONCAT(ROUND(avg(tripduration)/60, 2),' minutes') as avg_trip_duration
from `citibike_trips_modified.trips`
group by gender
order by avg_trip_duration desc;

--How many trips have been taken by subscribers/customers? ...makes sense that mroe subscribers would use service mroe
select usertype , count(usertype), '###,###' as user_count
from `citibike_trips_modified.trips`
group by usertype 
order by user_count desc;

--most popular start stations  
select start_station_name, start_station_id, count(start_station_id) as trip_started_count
from  `citibike_trips_modified.trips`
group by start_station_name, start_station_id
order by trip_started_count desc
limit 10;
    
--time seriess for the amount of trips that have been taken. Percentage change in day to day trips is also being calculated. Can be drilled down into hours. 
with daily_counts as (
  select
    extract(YEAR from starttime) as year,
    extract(MONTH from starttime) as month,
    extract(DAY from starttime) as day,
    count(*) as num_trips
  from`citibike_trips_modified.trips`
  group by year,month, day
  order by year asc, month asc, day asc),
daily_changes as (
  select year, month,day, num_trips,
    lag(num_trips) over (order by year, month, day) as prev_num_trips
  from daily_counts
)
select year, month, day, num_trips,
  case
    when prev_num_trips is null then null
    when prev_num_trips = 0 then null
    else concat(round((num_trips - prev_num_trips) / prev_num_trips * 100 , 2), '%')
  end as percentage_change
from daily_changes
order by year asc, month asc, day asc

--Average duration of trips by year
select 
  extract(YEAR from starttime) as year,
  round(avg(tripduration/60) , 2) as avg_trip_duration,
  count(*) as number_of_trips
from `citibike_trips_modified.trips`
group by year
order by year asc;

--Looking for the reason why 2018 is missing a large number of trips
select 
  extract(YEAR from starttime) as year,
  extract(MONTH from starttime) as month
from `citibike_trips_modified.trips`
where extract(YEAR from starttime) = 2018
group by year, month
order by month asc;