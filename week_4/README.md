# Week 4 DE Zoomcamp work

I've used dbt locally with dbt-bigquery plugin.
You can find the repo [here](https://github.com/inharsh/dbt-fhv-trips).

### Question 1

Query result: 58159513
```
select count(1) from glossy-attic-375106.dbt_hsingh.stg_fhv_tripdata
where extract(year from pickup_datetime) in (2019,2020);
```

### Question 2

Chart result - 10.3 : 89.7

<img src="https://github.com/inharsh/dezoomcamp-work/blob/main/week_4/images/que2.png" width="310" height="300">

### Question 3

stg_fhv_tripdata.sql

```
{{ config(materialized='view') }}

select
-- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as  dispatch_no,
    cast(PULocationID as integer) pickup_locationid,
    cast(DOLocationID as integer) dropoff_locationid,
    Affiliated_base_number as  affiliated_no,
    cast(SR_Flag as numeric) sr_flag,

-- timestamps
    cast(pickup_datetime as timestamp) pickup_datetime,
    cast(dropoff_datetime as timestamp) dropoff_datetime,

from {{ source('staging', 'fhv_tripdata') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
```

Query result: 43244696
```
select count(1) FROM `glossy-attic-375106.dbt_hsingh.stg_fhv_tripdata` 
where extract(year from pickup_datetime) = 2019;
```

### Question 4

fact_fhv_trips.sql
```
{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    dispatch_no,
    pickup_locationid,
    dropoff_locationid,
    affiliated_no,
    sr_flag,
    pickup_datetime,
    dropoff_datetime

from {{ ref('stg_fhv_tripdata') }} as stg_fhv

inner join dim_zones as pickup_zone
    on stg_fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on stg_fhv.dropoff_locationid = dropoff_zone.locationid
```

Query result: 22998722
```
select count(1) FROM `glossy-attic-375106.dbt_hsingh.fact_fhv_trips` 
where extract(year from pickup_datetime) = 2019;
```

### Question 5
Most busy month is January
<img src="https://github.com/inharsh/dezoomcamp-work/blob/main/week_4/images/que5.png" width="310" height="300">
