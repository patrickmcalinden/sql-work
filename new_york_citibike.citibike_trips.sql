--removes all of the null data assuming that no row can have a blank start station with other valid columns of data \\saving result of this as view in bigquery
select *
from bigquery-public-data.new_york_citibike.citibike_trips
where start_station_id is  Null;

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

select extract(year from starttime) as year, usertype, ROUND(avg(tripduration)/60, 2) as avg_trip_duration
from `citibike_trips_modified.trips`
group by year, usertype
order by avg_trip_duration desc;

--How many trips have been taken by subscribers/customers? ...makes sense that mroe subscribers would use service mroe
select usertype , count(usertype) as user_count, 
from `citibike_trips_modified.trips`
group by usertype 
order by user_count desc;

--trips by month
select extract(year from starttime) as year,
 extract(month from starttime) as month, 
 count(*) as tripcount, 
from `citibike_trips_modified.trips`
group by year, month
order by month desc;

--most popular start stations  
select start_station_name, start_station_latitude, start_station_longitude,  count(start_station_id) as trip_started_count
from  `citibike_trips_modified.trips`
group by start_station_name, start_station_latitude , start_station_longitude
order by trip_started_count desc;
    
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
  extract(month from starttime) as month,
  round(avg(tripduration/60) , 2) as avg_trip_duration,
  count(*) as number_of_trips
from `citibike_trips_modified.trips`
group by year, month
order by year, month asc;

--Looking for the reason why 2018 is missing a large number of trips
select 
  extract(YEAR from starttime) as year,
  extract(MONTH from starttime) as month
from `citibike_trips_modified.trips`
where extract(YEAR from starttime) = 2018
group by year, month
order by month asc;

select extract(year from starttime) as year, count(*) as trips
from `citibike_trips_modified.trips`
group by year

--What time of day has the most trips?

--first checking for dupliated entries (only one dup)
SELECT bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype, COUNT(*) as count
FROM `citibike_trips_modified.trips`
GROUP BY bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype
HAVING COUNT(*) > 1

SELECT EXTRACT(HOUR FROM starttime) AS hour , count(*) as tripsperhour
FROM (
    SELECT bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype, COUNT(*) as count
    FROM `citibike_trips_modified.trips`
    GROUP BY bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype
    HAVING COUNT(*) = 1
)
group by hour
order by hour asc

--Over the course of the dataset what month has the most trips
SELECT EXTRACT(MONTH FROM starttime) AS month , count(*) as tripspermonth
FROM (
    SELECT bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype, COUNT(*) as count
    FROM `citibike_trips_modified.trips`
    GROUP BY bikeid, starttime,stoptime,tripduration, start_station_id, end_station_id, usertype
    HAVING COUNT(*) = 1
)
group by month
order by month asc


SELECT
  EXTRACT(MONTH FROM starttime) AS month,
  EXTRACT(HOUR FROM starttime) AS hour,
  COUNT(*) AS trips
FROM (
  SELECT
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype,
    COUNT(*) AS count
  FROM `citibike_trips_modified.trips`
  GROUP BY
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype
  HAVING COUNT(*) = 1
)
GROUP BY month, hour
order by month, hour

SELECT 
  EXTRACT(YEAR FROM starttime) AS year,
  EXTRACT(MONTH FROM starttime) AS month,
  CASE 
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 18 AND 23 THEN 'Evening'
    ELSE 'Night'
  END AS time_segment,
  COUNT(*) AS trips
FROM (
  SELECT
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype,
    COUNT(*) AS count
  FROM `citibike_trips_modified.trips`
  GROUP BY
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype
  HAVING COUNT(*) = 1
)
GROUP BY year, month, time_segment
ORDER BY year, month, CASE time_segment
          WHEN 'Morning' THEN 1
          WHEN 'Afternoon' THEN 2
          WHEN 'Evening' THEN 3
          ELSE 4
          end

select *
from `citibike_trips_modified.trips`


--gender - when each gender takes the maority of their trips 
SELECT 
  gender,
  EXTRACT(HOUR FROM starttime) AS hour,
  COUNT(*) AS trips
FROM (
  SELECT
    bikeid,
    starttime,
    stoptime,
    tripduration,
    start_station_id,
    end_station_id,
    usertype,
    gender,
    COUNT(*) AS count
  FROM `citibike_trips_modified.trips`
  GROUP BY
    bikeid,
    starttime,
    stoptime,
    tripduration,
    start_station_id,
    end_station_id,
    usertype,
    gender
  HAVING COUNT(*) = 1
) 
GROUP BY gender, hour
ORDER BY gender, hour

--user type  -when each user type takes the majority of their trips
SELECT 
  usertype,
  CASE 
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM starttime) BETWEEN 18 AND 23 THEN 'Evening'
    ELSE 'Night'
  END AS time_segment,
  COUNT(*) AS trips
FROM (
  SELECT
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype,
    COUNT(*) AS count
  FROM `citibike_trips_modified.trips`
  GROUP BY
    bikeid, starttime, stoptime, tripduration,
    start_station_id, end_station_id, usertype
  HAVING COUNT(*) = 1
)
GROUP BY usertype,time_segment
ORDER BY usertype, CASE time_segment
          WHEN 'Morning' THEN 1
          WHEN 'Afternoon' THEN 2
          WHEN 'Evening' THEN 3
          ELSE 4
          end

--finiding distance traveled for each bike each trip 
SELECT extract(YEAR from starttime) as year, bikeid, 
  SUM(
    3963.189 * CASE 
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) > 1 THEN 0
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) < -1 THEN 0
      ELSE ACOS(
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) 
    END
  ) AS total_distance_in_miles
FROM `citibike_trips_modified.trips`
GROUP BY bikeid, year;

select COUNT(DISTINCT bikeid)
FROM `citibike_trips_modified.trips`


SELECT bikeid, 
  SUM(
    3963.189 * CASE 
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) > 1 THEN 0
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) < -1 THEN 0
      ELSE ACOS(
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) 
    END
  ) AS total_distance_in_miles
FROM `citibike_trips_modified.trips`
GROUP BY bikeid;

SELECT extract(year from starttime) as year, 
  SUM(
    3963.189 * CASE 
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) > 1 THEN 0
      WHEN (
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) < -1 THEN 0
      ELSE ACOS(
        SIN(start_station_latitude * 0.017453293) * SIN(end_station_latitude * 0.017453293) +
        COS(start_station_latitude * 0.017453293) * COS(end_station_latitude * 0.017453293) *
        COS((end_station_longitude * 0.017453293) - (start_station_longitude * 0.017453293))
      ) 
    END
  ) AS total_distance_in_miles
FROM `citibike_trips_modified.trips`
GROUP BY year;


select *
FROM `citibike_trips_modified.trips`

---gets distance and gcoutns all trips and time spent for each bike by year
select extract(year from starttime) as year,
       bikeid,
       count(*) as unique_trips,
       round(sum(tripduration)/60,2) as total_time_spent,
       sum(
         st_distance(
            st_geogpoint(start_station_longitude, start_station_latitude),
            st_geogpoint(end_station_longitude,   end_station_latitude)
          )/1609) as dist_in_miles
from  `citibike_trips_modified.trips`
group by year, bikeid
order by dist_in_miles desc

--gets count of trips and time spent riding
select bikeid, count(*) as trips , round(sum(tripduration)/60,2) as totaltime
from  `citibike_trips_modified.trips`
group by bikeid
order by totaltime desc

--getting unique bike count for year
select extract(year from starttime) as year,
       COUNT(DISTINCT bikeid) as unique_bike_count
FROM `citibike_trips_modified.trips`
group by year

---new code for gettting count for all rides a station sees
select extract(year from starttime) as year, extract(month from starttime) as month, start_station_name, start_station_latitude, start_station_longitude,  count(start_station_id) as trip_started_count
from  `citibike_trips_modified.trips`
group by year, month, start_station_name, start_station_latitude , start_station_longitude
order by trip_started_count desc;

--checking how many trips where 0 miles traveled , may indicate an error with ride
select extract(year from starttime) as year,
       extract(month from starttime) as month,
       count(*) as error_trips 
from (select *,
  st_distance(
    st_geogpoint(start_station_longitude, start_station_latitude),
    st_geogpoint(end_station_longitude,   end_station_latitude)
  )/1609 as dist_in_miles
from  `citibike_trips_modified.trips`)
where dist_in_miles = 0 and tripduration < 90
group by year, month

--statement to check to see if any null values for columns we will be using moving foward
select *
from `citibike_trips_modified.trips`
where starttime is not null
limit 5
--gets the average age of a usertype (not accurate do to the fact that we cannot identify any specific customer)
select year, usertype, avg(year - birth_year) as avgage_age,
from(
    select extract(year from starttime) as year, birth_year, usertype
  from `citibike_trips_modified.trips`
)
where birth_year is not null
group by year, usertype
order by year asc

--checking to get average birth year of user type
select extract(year from starttime) as year, usertype, avg(birth_year) as avgage_birth_year
from `citibike_trips_modified.trips`
group by year, usertype
order by year asc

--gets the average age of a gender (not accurate do to the fact that we cannot identify any specific customer)
select year, gender, avg(year - birth_year) as avgage_age,
from(
    select extract(year from starttime) as year, birth_year, gender
  from `citibike_trips_modified.trips`
)
where gender is not null
group by year, gender
order by year asc

--find the percentage of users that did not supply birth year
SELECT year,
       sum(CASE WHEN birth_year IS NULL THEN 1 END) AS userwithnoage,
       COUNT(CASE WHEN birth_year IS NOT NULL THEN 1 END) AS userwithage,
       ROUND(COUNT(CASE WHEN birth_year IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS null_percent
FROM (
  SELECT EXTRACT(YEAR FROM starttime) AS year, birth_year
  FROM `citibike_trips_modified.trips`
)
GROUP BY year
ORDER BY year ASC
