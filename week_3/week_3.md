# Week 3 DE Zoomcamp work

Prefect flow to write data onto GCP Bucket

![Alt text](https://github.com/inharsh/dezoomcamp-work/blob/main/week_3/images/Screenshot%202023-02-13%20173002.png?raw=true "Prefect_flow")


Create external table from fhv 2019 data

```
create or replace external table `glossy-attic-375106.decamp_bq.external_fhv_2019`
options (
  format = 'CSV',
  uris = ['gs://decamp-bq/fhv/2019/fhv_tripdata_*.csv.gz']
);
```

Create non-partitioned table from external table created

```
create or replace table `glossy-attic-375106.decamp_bq.fhv_2019_non_partitioned` as
select * from `glossy-attic-375106.decamp_bq.external_fhv_2019`;
```

### Question 1:

```select count(1) from `glossy-attic-375106.decamp_bq.fhv_2019_non_partitioned`;```

43,244,696 rows

### Question 2:

```
select count(distinct(fhv.Affiliated_base_number)) from glossy-attic-375106.decamp_bq.external_fhv_2019 as fhv;

select count(distinct(fhv.Affiliated_base_number)) from glossy-attic-375106.decamp_bq.fhv_2019_non_partitioned as fhv;
```

0 MB for the External Table and 317.94MB for the Materialized Table

### Question 3:

```
select count(1) from glossy-attic-375106.decamp_bq.fhv_2019_non_partitioned as fhv
where fhv.PULocationID is NULL and fhv.DOLocationID is NULL;
```

717,748 records has PUlocationID and DOlocationID NULL

### Question 4:

```
create or replace table `glossy-attic-375106.decamp_bq.fhv_2019_partitioned_clustered`
partition by date(pickup_datetime)
cluster by Affiliated_base_number as
select * from `glossy-attic-375106.decamp_bq.external_fhv_2019`;
```
Partition by pickup_datetime as filter operations are done on this column, Cluster on affiliated_base_number as sort operation is done has 3163 distinct records

### Question 5:

```
select distinct(fhv.Affiliated_base_number)
from glossy-attic-375106.decamp_bq.fhv_2019_non_partitioned fhv
where date(fhv.pickup_datetime) between '2019-03-01' and '2019-03-31';

select distinct(fhv.Affiliated_base_number)
from glossy-attic-375106.decamp_bq.fhv_2019_partitioned_clustered fhv
where date(fhv.pickup_datetime) between '2019-03-01' and '2019-03-31';
```

647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

### Question 6:

GCP Bucket, since the external table we are creating is using dataset stored in GCP Bucket

### Question 7:

Clustering is not always preferred, for eg. the cost upfront is unknown
