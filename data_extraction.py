#import database_utils as db
import pandas as pd
import tabula
import requests
import json
import boto3
#import sys
from io import StringIO



class DataExtractor:
    '''This class works as a utility class, it has methods that help extract data from different data sources.
    Methods:

    The methods extract data from a particular data source, these sources include CSV files, an API and an S3 bucket.
    read_rds_table(self, connector, table):
        Reads SQL table from RDS database and returns table as pandas DataFrame.
    retrieve_pdf_data(self, pdf_name):
        Retrieves tabular data from cloud-based .pdf file and returns data as pandas DataFrame.
    list_number_of_stores(self, endpoint, header):
        Retrieves number of stores from api endpoint.
    retrieve_stores_data(self,header, number_of_stores):
        Iteratively retrieves individual store records and adds them to pandas DataFrame.
    extract_from_s3(self,bucketname, objectkey):
        Retrieves data from .json or .csv file stored in AWS S3.
    extract_from_s3_datetime(self, bucket_name, object_key)
        Retrieves datetime data stored in csv format from S3 bucket on AWS
    '''
    

    def __init__(self):
        pass


    def read_rds_table(self,engine, tables):
        '''  This method extracts the table containing user data and return a pandas DataFrame.
        Args:
            engine: sqlalchemy database engine
            tables: name of the table to read from database
            
        Returns:
            dataframe'''
        
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            dfusers = pd.read_sql_table(tables,conn)
        return dfusers
        
    #PDF
    def retrieve_pdf_data(self, pdf_name):
        ''' This method extracts data(card details) from the all pages of the pdf document
        Args:
            pdf_name: name of pdf file
        Returns:
            dataframe'''
        #tabula returns the list of dataframes, therefore concatenate all the dataframes
        cards_df = pd.concat(tabula.read_pdf(pdf_name,pages='all',multiple_tables=True))
        return cards_df
    
    #API
    def list_number_of_stores(self, endpoint,header):
        ''' This method extracts the store data through the API
        Args: 
            endpoint: URL address to get number of stores
            header(dictionary) : contains key details
        Returns
            number of stores:
        '''
        response = requests.get(url=endpoint,headers=header)
        data = response.json()
        #print(data)
        return data['number_stores']
    
    def retrieve_stores_data (self,header, num_of_stores):
        '''This method extracts all the store details using url address and header dictionary
        Args:
            header(dictionary): contains key information
            number_of_stores(int): number of stores whose data need to be extracted
        Returns:
            Dataframe'''
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
        ''' This method extracts data(product data) stored in csv format from S3 bucket on AWS
        It uses boto3 package to download
        Args:
            bucket_name: name of the  S3 bucket on AWS
            object_key: name of the file on bucket
            
        Returns: 
            Dataframe'''
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
        ''' This method extracts data(date-time data) stored in csv format from S3 bucket on AWS
        It uses boto3 package to download
        Args:
            bucket_name: name of the  S3 bucket on AWS
            object_key: name of the file on bucket
            
        Returns: 
            Dataframe'''
        client = boto3.client('s3')

        bucket_name = 'data-handling-public'

        object_key = 'date_details.json'
        json_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        body = json_obj['Body']
        json_string = body.read().decode('utf-8')

        dates_df = pd.read_json(StringIO(json_string))

        return dates_df

            

