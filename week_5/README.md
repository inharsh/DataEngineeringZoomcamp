### Question 1: 3.3.2
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
```

### Question 2: 24MB

```
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-06.csv.gz')
    
df = df.repartition(12)

df.write.parquet('fhvhv/2021/06')
```

```
(base) harshit@decamp:~/notebooks$ ls -lh fhvhv/2021/06/
total 284M
-rw-r--r-- 1 harshit harshit   0 Feb 25 18:15 _SUCCESS
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00000-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00001-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00002-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00003-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00004-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00005-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00006-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00007-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00008-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00009-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00010-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
-rw-r--r-- 1 harshit harshit 24M Feb 25 18:15 part-00011-bebf8728-133c-4ec1-a328-c2a1a3b81091-c000.snappy.parquet
```

### Question 3: 452470

Import col function
```
from pyspark.sql.functions import col
```
```
df \
    .withColumn("pickup_date", to_date(col('pickup_datetime'))) \
    .filter(col("pickup_date") == "2021-06-15") \
    .count() 
    
452470

```
### Question 4: 66.88 Hours

```
df.registerTempTable('fhv_rides')

spark.sql("""
select to_date(pickup_datetime) pickup_date, round(cast((dropoff_datetime - pickup_datetime) as long) / 3600, 2) time_diff
from fhv_rides
order by time_diff desc
limit 1;
""").show()

+-----------+---------+
|pickup_date|time_diff|
+-----------+---------+
| 2021-06-25|    66.88|
+-----------+---------+
```

### Question 5: 4040

```
(base) harshit@decamp:~/notebooks$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/03/04 09:48:30 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://decamp.asia-south2-a.c.lushy-greens-2324363.internal:4040
```

### Question 6: Crown Heights North

Joining Zone table with fhvhv
```
df_join = df_zones.join(df, df_zones.LocationID == df.PULocationID)
```

```
df_zones.registerTempTable('zones')

spark.sql("""
select f.PULocationID, z.Zone
from zones z
join fhv_rides f on f.PULocationID = z.LocationID
group by 1, 2
order by count(f.PULocationID) desc
limit 1;
""").show()

[Stage 59:>                                                         (0 + 4) / 4]
+------------+-------------------+
|PULocationID|               Zone|
+------------+-------------------+
|          61|Crown Heights North|
+------------+-------------------+
```
