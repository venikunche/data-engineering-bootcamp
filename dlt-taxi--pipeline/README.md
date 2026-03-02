## Question 1: Start and end date of the dataset
```
SELECT 
    MIN(trip_pickup_date_time) AS start_date, 
    MAX(trip_pickup_date_time) AS end_date 
FROM taxi_data.rides;
```

## Question 2: Proportion of trips paid with credit card
```
SELECT 
    ROUND(100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*), 2) AS credit_card_percentage
FROM taxi_data.rides;
```

## Question 3: Total tips
```
SELECT ROUND(SUM(tip_amt), 2) AS total_tips FROM taxi_data.rides;
```