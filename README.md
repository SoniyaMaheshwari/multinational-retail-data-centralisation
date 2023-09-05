#  **Multinational-Retail-Data-Centralisation**
In this project, we clean and analyze the sales data of a multinational retail store, which has many different data sources.

Here, we create a local PostgreSQL database, that stores the current company data so that it's accessed from one centralised location and acts as a single source of truth for sales data. We create a database schema and run SQL queries.

Key technologies used: Python (Pandas),Postgres, AWS (s3), boto3, rest-API, csv.

## **Project Utils**

1. **Data Extraction:** In data_extraction.py, we create a class named DataExtractor.This class will work as a utility class, in it we will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.

2. **Data Cleaning:**  In data_cleaning.py,this script will contain a class DataCleaning with methods to clean data from each of the data sources.

3. **Uploading data:** In database_utils.py, this script will contain a class DatabaseConnector which  will  be used to connect with and upload data to the database, based on the credentials provided in ".yaml" file.


## **Data Processing**
We extract data from various different data sources

1. AWS RDS database: The historical data of users is currently stored in an AWS database in the cloud. The data is extracted, cleaned and strored in the table named "dim_users" on local postgreSQL database.

2. AWS S3 bucket: The users card details are stored in a PDF document in an AWS S3 bucket.We use the tabula-py Python package, imported with tabula to extract all pages from the pdf document Then return a DataFrame of the extracted data. The data is cleaned and stored in "dim_card_details" table on local database.

3. The restful-API:The store data can be retrieved through the use of an API.The API has two GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number.To connect to the API you will need to include the API key to connect to the API in the method header. The ".json" response has to be converted into the pandas dataframe. The data is cleaned and stored in "dim_store_details" table on local database.

4. AWS s3 bucket: The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS. We use boto3 package to download and extract the information returning a pandas DataFrame.The data is cleaned and stored in "dim_products" table on local database.

5. AWS RDS database: This table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.The data is extracted, cleaned and strored in the table named "dim_orders" on local postgreSQL database.

6. AWS s3 bucket: The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.The file is currently stored on S3.The data is cleaned and stored in "dim_date_times" table on local database.

 ## ***Create the database schema**
 Create the star_based schema of the database, ensuring that the columns are of the correct data types. 

  ```
    ALTER TABLE dim_orders
        ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
        ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
        ALTER COLUMN card_number TYPE Varchar(40);
        ALTER COLUMN store_code TYPE Varchar(40);
        ALTER COLUMN product_code TYPE Varchar(40);
        ALTER COLUMN product_quantity TYPE SMALLINT;

 ```

 Add primary keys and foreign keys

 ```
   ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);

   ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_date_times FOREIGN KEY (date_uuid) 
	REFERENCES dim_date_times (date_uuid);

 ```

 Add additional columns with conditional data segmentation.

```
   
    ALTER TABLE dim_products
        ADD weight_class VARCHAR(50);


    UPDATE dim_products
    SET weight_class = CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND "weight" < 40 THEN 'Mid_sized'
        WHEN weight >= 40 AND "weight" < 140 THEN 'Heavy'
        WHEN weight >=140 THEN 'Truck_Required' 
        END
 ```

**Now we have the schema for the database and all the sales data in one location.**
**We can now get up-to-date metrics from data that can help business to make more data driven decisions and get a better understanding of sales**

## **Data Analysis using SQL queries**
1. How many stores does the business have and in which countries?
```
    SELECT country_code, COUNT(store_code) 
	FROM dim_store_details
	GROUP BY country_code;
```

![1](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/8b4d0b39-5a98-47c3-b507-44fdffb5c743)


2.   Which locations currently have most stores?
```SELECT locality, COUNT(store_code) as total_no_stores 
	FROM dim_store_details
	GROUP BY locality
	HAVING COUNT(store_code) >= 10
	ORDER BY COUNT(store_code) DESC; 


```
![2](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/76f70a5f-18c1-48de-bb3e-664f41c5da9d)


3. Which months produce highest cost of sales typically?
```WITH cte AS (SELECT dimdt.month,SUM(dimo.product_quantity*dimp.product_price) AS total_sales 
	FROM dim_orders AS dimo
	JOIN dim_date_times AS dimdt ON dimo.date_uuid = dimdt.date_uuid
	JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
	GROUP BY dimdt.month
	ORDER BY total_sales DESC)
   SELECT month, ROUND(total_sales :: numeric,2) AS total_sales FROM cte	;  


```

![3](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/ebab18ca-5f95-42bf-a89e-3e9bd7eb1788)


4. How many sales are coming from online?
```SELECT CASE
	WHEN store_type = 'Web Portal' THEN 'Web'
	ELSE 'Offline'
	END  AS store_types,
        SUM(product_quantity) AS product_quantity_count,
        COUNT(*) AS number_of_sales
    FROM dim_orders AS dimo
		JOIN dim_store_details AS dimst ON dimo.store_code=dimst.store_code
    GROUP BY CASE
        WHEN store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
        END   
```

![4](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/3ddae564-e206-4c99-be27-a38d2f243641)


5. What percentage of sales come through each type of store?
```WITH calculate_total_sales AS
    (SELECT 
        store_type,SUM(product_quantity*product_price) AS total_sales
    FROM 
        dim_orders AS dimo 
    JOIN dim_store_details AS dimst ON dimo.store_code = dimst.store_code
    JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
    GROUP BY store_type),

    calculate_percentage AS
    (SELECT 
        store_type,total_sales,(total_sales*100)/(select SUM(total_sales) FROM calculate_total_sales) AS "percent_total(%)" 
    FROM 
        calculate_total_sales
    GROUP BY 
        store_type, total_sales)
    SELECT 
        store_type, 
        ROUND(total_sales::numeric,2) AS total_sales,ROUND("percent_total(%)"::numeric, 2) AS "percent_total(%)" 
    FROM 
        calculate_percentage
    ORDER BY "percent_total(%)" DESC;

```

![5](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/07dded30-b049-424b-a8bd-e701ee7bb8cd)


6. Which month in each year produced the highest cost of sales?
```
WITH calculate_total_sales AS
(SELECT 
    year,
    month,
    SUM(product_quantity*product_price) AS total_sales
FROM 
    dim_orders AS dimo
    JOIN dim_date_times AS dimdt ON dimo.date_uuid = dimdt.date_uuid
    JOIN dim_products as dimp ON dimo.product_code = dimp.product_code
GROUP BY 
    year,month
ORDER BY year), 
create_partitions AS
(SELECT year,
		month,
		ROW_NUMBER() OVER(PARTITION BY YEAR ORDER BY total_sales DESC) AS rank,
		total_sales
FROM 
    calculate_total_sales
GROUP BY 
    year,month, total_sales
ORDER BY 
    year, rank DESC)
SELECT 
    year,month,total_sales
FROM 
    create_partitions
WHERE 
    rank = 1
ORDER BY 
    total_sales DESC;
```

       
![6](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/ec9e09d0-efb4-4416-8907-59e4a92f9196)

7. What is staff headcount?
```SELECT 
        country_code,SUM(staff_numbers) as staff_headcount
   FROM 
        dim_store_details
   GROUP BY 
        country_code;
```

![7](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/6ac4ecc2-11ae-492e-8169-81841c470e8d)


8. Which German store type is selling the most?
``` WITH cte AS 
    (SELECT 
        country_code,
        store_type,
        SUM(product_quantity*product_price) AS total_sales
    FROM 
        dim_orders AS dimo 
        JOIN dim_store_details AS dimst ON dimo.store_code = dimst.store_code
        JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
    WHERE 
        country_code='DE'
    GROUP BY 
        store_type, country_code)
    SELECT 
        country_code, store_type, ROUND(total_sales::numeric, 2) AS total_sales 
    FROM 
        cte
    ORDER BY 
        total_sales;
```


![8](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/1086ab33-bf60-4259-a8d8-e160157fbbe3)

9. How quickly is the company making the sales?
``` WITH cte AS(
    SELECT 
        year,month,day, timestamp,
	    lead(timestamp) OVER(PARTITION BY year,month,day ORDER BY timestamp) AS lead_time
    FROM 
        dim_date_times
    GROUP BY 
        year,month, day ,timestamp), 
    average AS(
        SELECT year,
        avg(lead_time-timestamp) as avg_sales_time
        FROM 
            cte
        GROUP BY year), 
    format AS(
        SELECT 
            year,
            EXTRACT(HOURS FROM avg_sales_time) AS hours,
            EXTRACT(MINUTES FROM avg_sales_time) AS minutes,
            FLOOR(EXTRACT(seconds FROM avg_sales_time))::int seconds,
            FLOOR(EXTRACT(milliseconds from avg_sales_time))::int - 1000 * FLOOR(EXTRACT(seconds from avg_sales_time))::int milliseconds
        FROM average) 
    SELECT year, ' "hours": ' || hours 
            || ' "minutes": ' || minutes
            || ' "seconds": ' || seconds
            || ' "milliseconds": ' ||  milliseconds 
    FROM format ;
```


![9](https://github.com/SoniyaMaheshwari/multinational-retail-data-centralisation/assets/139882461/ecd6ccdb-97a3-4f3f-b90d-4e8cc92ca5e5)