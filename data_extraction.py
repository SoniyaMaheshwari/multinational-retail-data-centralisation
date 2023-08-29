#import database_utils as db
import pandas as pd
import tabula
import requests
import json
import boto3
#import sys
from io import StringIO



class DataExtractor:
    def __init__(self):
        pass


    def read_rds_table(self,engine, tables):
  
        #self.tables = dbcon.list_db_tables()
        #print(self.tables)
        #engine = dbcon.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            dfusers = pd.read_sql_table(tables,conn)
        return dfusers
        
    #PDF
    def retrieve_pdf_data(self, pdf_name):
        #tabula returns the list of dataframes, therefore concatenate all the dataframes
        cards_df = pd.concat(tabula.read_pdf(pdf_name,pages='all',multiple_tables=True))
        return cards_df
    
    #API
    def list_number_of_stores(self, endpoint,header):
        response = requests.get(url=endpoint,headers=header)
        data = response.json()
        #print(data)
        return data['number_stores']
    
    def retrieve_stores_data (self,header, num_of_stores):
        store_df =pd.DataFrame()
        for i in range(0, num_of_stores):
            endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}"
            response = requests.get(url=endpoint,headers=header)
            data = response.json()
            #df=df.append(data)
            new_df = pd.DataFrame(data, index =[0])
            store_df = pd.concat([store_df,new_df],ignore_index=True)

        return store_df
    
    def extract_from_s3(self,bucket_name, object_key):
        client = boto3.client('s3')
        #path = 's3://data-handling-public/products.csv'
        #products_df = pd.read_csv(path)
        #bucket_name = 'data-handling-public'

        #object_key = 'products.csv'
        csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')

        products_df = pd.read_csv(StringIO(csv_string))

        return products_df


    def extract_from_s3_datetime(self, bucket_name, object_key):
        client = boto3.client('s3')

        bucket_name = 'data-handling-public'

        object_key = 'date_details.json'
        json_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        body = json_obj['Body']
        json_string = body.read().decode('utf-8')

        dates_df = pd.read_json(StringIO(json_string))

        return dates_df

            

