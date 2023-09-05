from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


dbcon = DatabaseConnector()
dataex = DataExtractor()
dataclean = DataCleaning()

#Extract, clean and upload user details from yaml file
engine = dbcon.init_db_engine('db_creds.yaml') # get sqlalchemy database engine to estabilish connection to RDS
tables=dbcon.list_db_tables(engine) # get list of tables from RDS
dfusers=dataex.read_rds_table(engine,tables[1]) # read table from RDS containing user info
dfusers = dataclean.clean_user_data(dfusers) # clean user data 

engine_sales_data = dbcon.init_db_engine('postgres_creds.yaml') # initialize engine for postgresql sales_data database

dbcon.upload_to_db(engine_sales_data,dfusers,'dim_users') # upload user data to sales_data database table

#Extract, clean and upload card details from pdf file
cardsdf = dataex.retrieve_pdf_data('card_details.pdf')
cards_df = dataclean.clean_card_details(cardsdf)
dbcon.upload_to_db(engine_sales_data,cards_df,'dim_card_details')


#Extract, clean and upload store details using API
endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX" }
num_of_stores = dataex.list_number_of_stores(endpoint,headers)

#endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}"
store_df = dataex.retrieve_stores_data(headers,num_of_stores)

store_df = dataclean.clean_store_data(store_df)

dbcon.upload_to_db(engine_sales_data,store_df, 'dim_store_details')

#Extract, clean and upload product details boto3 s3 AWS
bucket_name = 'data-handling-public'
object_key = 'products.csv'
products_df = dataex. extract_from_s3(bucket_name,object_key)
#path = 's3://data-handling-public/products.csv'

products_df = dataclean.convert_product_weights(products_df)
products_df = dataclean. clean_products_data(products_df)
dbcon.upload_to_db(engine_sales_data,products_df, 'dim_products')
#print(tables)'''

#AWS RDS
orders_df=dataex.read_rds_table(engine,tables[2])
orders_df= dataclean.clean_orders_data(orders_df)
dbcon.upload_to_db(engine_sales_data, orders_df, 'dim_orders')

#Exttract and clean and upload dates table
#path = https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
bucket_name = 'data-handling-public'
object_key = 'date_details'
dates_df = dataex. extract_from_s3_datetime(bucket_name,object_key)
dates_df = dataclean.clean_date_times(dates_df)
dbcon.upload_to_db(engine_sales_data,dates_df,'dim_date_times')