# Module 3: Data Warehousing Homework

## To create an external table
```
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-zoomcamp-hw3.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data-engineering-zoomcamp-hw3-2026/yellow_tripdata_2024-*.parquet']
);
```
## To create a regular table in BQ without partitioning
```
CREATE OR REPLACE TABLE data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM data-engineering-zoomcamp-hw3.nytaxi.external_yellow_tripdata;
```

## Q1 Counting records
```
SELECT count(*) FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned;
```

## Q2 Data read estimation
```
-- For External Table
SELECT COUNT(DISTINCT(PULocationID)) FROM data-engineering-zoomcamp-hw3.nytaxi.external_yellow_tripdata;

-- For Materialized
SELECT COUNT(DISTINCT(PULocationID)) FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned;
```

## Q3 Understanding columnar storage
```
-- One column
SELECT PULocationID FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned;

-- Two columns
SELECT PULocationID, DOLocationID FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned;
```

## Q4 Counting zero fare trips
```
SELECT count (*) FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;
```

## Q5 Partitioning and clustering
```
CREATE OR REPLACE TABLE data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM data-engineering-zoomcamp-hw3.nytaxi.external_yellow_tripdata;
```

## Q6 Partition benefits
```
-- For non-partitioned
SELECT DISTINCT(VendorID)
FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- For partitioned
SELECT DISTINCT(VendorID)
FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

## Q9 Understanding table scans
```
SELECT COUNT(*)
FROM data-engineering-zoomcamp-hw3.nytaxi.yellow_tripdata_partitioned_clustered;
```