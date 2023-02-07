--Q3 How many taxi trips were totally made on January 15?

select count(1)
from green_taxi_data t
where
	date(t.lpep_pickup_datetime) = '2019-01-15'
	and
	date(t.lpep_dropoff_datetime) = '2019-01-15'
;

--Q4 Which was the day with the largest trip distance Use the pick up time for your calculations.

select
	date(t.lpep_pickup_datetime) putime 
from green_taxi_data t
group by putime
order by max(t.trip_distance) desc
limit 1;

--Q5 In 2019-01-01 how many trips had 2 and 3 passengers?

select
	format('2: %s ; 3: %s',
	(select count(1)
	from green_taxi_data
	where
		passenger_count = 2
		and 
		date(lpep_pickup_datetime) = '2019-01-01'),
	(select count(1)
	from green_taxi_data
	where
		passenger_count = 3
		and 
		date(lpep_pickup_datetime) = '2019-01-01')) ans
;

--Q6 For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

select zdo."Zone"
from green_taxi_data t
join zones zpu on
	t."PULocationID" = zpu."LocationID"
join zones zdo on
	t."DOLocationID" = zdo."LocationID"
where zpu."Zone" = 'Astoria'
order by t.tip_amount desc
limit 1;
