/*Changing data types , for uuid you need to use USING syntax*/
ALTER TABLE dim_orders
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;
    ALTER COLUMN card_number TYPE Varchar(40);
    ALTER COLUMN store_code TYPE Varchar(40);
	ALTER COLUMN product_code TYPE Varchar(40);
	ALTER COLUMN product_quantity TYPE SMALLINT;


ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE Varchar(40);
	ALTER COLUMN last_name TYPE Varchar(40);
    ALTER COLUMN date_of_birth TYPE DATE;
	ALTER COLUMN country_code TYPE Varchar(10);
	ALTER COLUMN join_date TYPE DATE;
	ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;
    ALTER COLUMN locality TYPE VARCHAR(25);	
    ALTER COLUMN store_code TYPE VARCHAR(25);	
	ALTER COLUMN address TYPE VARCHAR(255);	
	ALTER COLUMN staff_numbers TYPE SMALLINT;	
	ALTER COLUMN opening_date TYPE DATE;
	ALTER COLUMN locality TYPE VARCHAR(25);	
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;
	
ALTER TABLE dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(255);
	ALTER COLUMN continent TYPE VARCHAR(25);
	ALTER COLUMN store_type TYPE VARCHAR(255);
	ALTER COLUMN store_type DROP NOT NULL;
	
	
		
UPDATE dim_store_details
	SET locality = 'N/A'
	WHERE locality IS NULL;

ALTER TABLE dim_products
ADD weight_class VARCHAR(50);


UPDATE dim_products
SET weight_class = CASE 
	  WHEN weight < 2 THEN 'Light'
      WHEN weight >= 2 AND "weight" < 40 THEN 'Mid_sized'
      WHEN weight >= 40 AND "weight" < 140 THEN 'Heavy'
      WHEN weight >=140 THEN 'Truck_Required' 
    END 

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid;
    ALTER COLUMN "EAN" TYPE VARCHAR(50) ;
    ALTER COLUMN product_code TYPE VARCHAR(50); 
    ALTER COLUMN date_added TYPE DATE; 

ALTER TABLE dim_products 
	ALTER still_available TYPE bool 
		USING CASE WHEN still_available='Removed' THEN FALSE ELSE TRUE END;
		
DELETE FROM dim_products WHERE product_name = 'LB3D71C025';
DELETE FROM dim_products WHERE product_name = 'VLPCU81M30';
						
ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
    ALTER COLUMN month TYPE VARCHAR(15); 
	ALTER COLUMN year TYPE VARCHAR(15); 
    ALTER COLUMN day TYPE VARCHAR(15); 
	ALTER COLUMN time_period TYPE VARCHAR(15); 
    ALTER COLUMN card_number TYPE VARCHAR(30); 
    ALTER COLUMN expiry_date TYPE VARCHAR(15); 
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date; 
	

DELETE FROM dim_date_times 
	WHERE date_uuid IS NULL;
	
ALTER TABLE dim_date_times ALTER COLUMN date_uuid SET NOT NULL;	
	
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);

DELETE FROM dim_users 
	WHERE user_uuid IS NULL;
	
ALTER TABLE dim_users ALTER COLUMN user_uuid SET NOT NULL;

ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
    
ALTER TABLE dim_card_details ALTER COLUMN card_number SET NOT NULL;

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
    
	
ALTER TABLE dim_store_details ALTER COLUMN store_code SET NOT NULL;

ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
    
ALTER TABLE dim_products ALTER COLUMN product_code SET NOT NULL;

ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
    
/*DELETE FROM dim_orders WHERE date_uuid NOT IN 
(SELECT date_uuid FROM dim_date_times)*/

/* NOT IN is taking 3 hours to execute, instead use LEFT JOIN
AS SHOWN BELOW, IT TAKES FEW SECONDS*/

/* USE UPDATE AND SET VALUES TO NULL INSTEAD OF DELETING THEM*/

UPDATE dim_orders
SET date_uuid = NULL
WHERE date_uuid IN
(SELECT dim_orders.date_uuid FROM dim_orders	
LEFT JOIN  dim_date_times 
ON dim_orders.date_uuid = dim_date_times.date_uuid
WHERE dim_date_times.date_uuid IS NULL);


ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_date_times FOREIGN KEY (date_uuid) 
	REFERENCES dim_date_times (date_uuid);

UPDATE dim_orders
SET user_uuid = NULL
WHERE user_uuid NOT IN 
(SELECT user_uuid FROM dim_users)

ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_users FOREIGN KEY (user_uuid) 
	REFERENCES dim_users (user_uuid);

ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_card_details FOREIGN KEY (card_number) 
	REFERENCES dim_card_details (card_number);

ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_card_details FOREIGN KEY (card_number) 
	REFERENCES dim_card_details (card_number);

ALTER TABLE dim_orders
    ADD CONSTRAINT fk_orders_card_details FOREIGN KEY (card_number) 
	REFERENCES dim_card_details (card_number);

SELECT country_code, COUNT(store_code) 
	FROM dim_store_details
	GROUP BY country_code;

SELECT locality, COUNT(store_code) as total_no_stores 
	FROM dim_store_details
	GROUP BY locality
	HAVING COUNT(store_code) >= 10
	ORDER BY COUNT(store_code) DESC; 

WITH cte AS (SELECT dimdt.month,SUM(dimo.product_quantity*dimp.product_price) AS total_sales 
	FROM dim_orders AS dimo
	JOIN dim_date_times AS dimdt ON dimo.date_uuid = dimdt.date_uuid
	JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
	GROUP BY dimdt.month
	ORDER BY total_sales DESC)
SELECT month, ROUND(total_sales :: numeric,2) FROM cte	;  


SELECT CASE
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

WITH calculate_total_sales AS
(SELECT store_type,SUM(product_quantity*product_price) AS total_sales
FROM dim_orders AS dimo 
JOIN dim_store_details AS dimst ON dimo.store_code = dimst.store_code
JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
GROUP BY store_type),
calculate_percentage AS
(SELECT store_type,total_sales,(total_sales*100)/(select SUM(total_sales) FROM calculate_total_sales) AS "percent_total(%)" FROM calculate_total_sales
GROUP BY store_type, total_sales)
SELECT store_type, ROUND(total_sales::numeric,2) AS total_sales,ROUND("percent_total(%)"::numeric, 2) AS "percent_total(%)" FROM calculate_percentage
ORDER BY "percent_total(%)" DESC;

	        
		

WITH calculate_total_sales AS
(SELECT year,month,SUM(product_quantity*product_price) AS total_sales
FROM dim_orders AS dimo
JOIN dim_date_times AS dimdt ON dimo.date_uuid = dimdt.date_uuid
JOIN dim_products as dimp ON dimo.product_code = dimp.product_code
GROUP BY year,month
ORDER BY year), create_partitions AS
(SELECT year,
		month,
		ROW_NUMBER() OVER(PARTITION BY YEAR ORDER BY total_sales DESC) AS rank,
		total_sales
FROM calculate_total_sales
GROUP BY year,month, total_sales
ORDER BY year, rank DESC)
SELECT year,month,total_sales
FROM create_partitions
WHERE rank = 1
ORDER BY total_sales DESC;
       

SELECT country_code,SUM(staff_numbers) as staff_headcount
FROM dim_store_details
GROUP BY country_code;


WITH cte AS (SELECT country_code,store_type,SUM(product_quantity*product_price) AS total_sales
FROM dim_orders AS dimo 
JOIN dim_store_details AS dimst ON dimo.store_code = dimst.store_code
JOIN dim_products AS dimp ON dimo.product_code = dimp.product_code
WHERE country_code='DE'
GROUP BY store_type, country_code)
SELECT country_code, store_type, ROUND(total_sales::numeric, 2) AS total_sales 
FROM cte
ORDER BY total_sales;


WITH cte AS(SELECT year,month,day, timestamp,
	 lead(timestamp) OVER(PARTITION BY year,month,day ORDER BY timestamp) AS lead_time
FROM dim_date_times
GROUP BY year,month, day ,timestamp), average AS
(SELECT year,avg(lead_time-timestamp) as avg_sales_time
FROM cte
GROUP BY year), 
format AS
(SELECT year,
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
