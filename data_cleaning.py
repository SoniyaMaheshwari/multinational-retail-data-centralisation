
import database_utils as db
import data_extraction as de
import pandas as pd
import numpy as np
import re

class DataCleaning:
    '''This class contains methods for cleaning data from various sources
    
    Methods:
    
    clean_user_data(self, dataframe):
        Cleans DataFrame containing business user data.
    clean_card_data(self, dataframe):
        Cleans DataFrame containing credit card data from business transactions.
    clean_store_data(self, dataframe):
        Cleans DataFrame containing details of each of the business' stores
    convert_product_weights(self, dataframe):
        Converts values in weight column of DataFrame to kilograms and to type floating point number
    clean_products_data(self, dataframe):
        Cleans DataFrame containing information about all products sold by the business.
    clean_orders_data(self, dataframe):
        Cleans main orders DataFrame.
    clean_date_times(self,dataframe):
        Cleans Dataframe containing date events data for all orders received by the business.
    '''

    def __init__(self):
        pass

    def clean_user_data(self,dfusers):
        #dfusers = dataex.read_rds_table(dbcon)
        ''' This method cleans user table extracted from RDS database'''

       # clean invalid dates
        dfusers = self.clean_date(dfusers,'join_date')
        dfusers = self.clean_date(dfusers,'date_of_birth')
       # clean_country_code column
        dfusers['country_code'] = dfusers['country_code'].astype('string')
        mapping = {'GGB':'GB'}
        dfusers = self.replace_invalid_strings_to_null(dfusers,'country_code')
        dfusers['country_code'] = dfusers['country_code'].replace(mapping)
        dfusers['country_code'] = dfusers['country_code'].replace('', pd.NA) 
        dfusers.replace('NULL', pd.NA, inplace=True)
        dfusers.dropna(axis='index',how = 'all', subset=['first_name','last_name'], inplace=True)
        # cleaning phone number
        # remove country codes and/or extensions from phone numbers
        dfusers['phone_number'] = dfusers['phone_number'].str.replace('\+1|\+44|\+49|x\w+', '', regex=True)
        # remove non-numeric characters from phone numbers
        dfusers['phone_number'] = dfusers['phone_number'].str.replace('\D+', '', regex=True)
        # strip remaining country codes from US numbers
        dfusers['phone_number'] = dfusers.apply(lambda x: x['phone_number'][-10:] if x['country_code'] == 'US' else x['phone_number'], axis=1)
        # add missing preceding '0' to GB numbers
        dfusers['phone_number'] = dfusers.apply(lambda x: '0' + x['phone_number'] if x['country_code'] == 'GB' and x['phone_number'][0] != '0' else x['phone_number'], axis=1) 
        # drop rows where unique user id is not standard 36 characters in length
        dfusers.drop(dfusers[dfusers['user_uuid'].str.len() != 36].index, inplace=True)
        return dfusers  

    def clean_card_details(self, cards_df):
        '''This method cleans data extracted from pdf document'''
        cards_df.replace('',pd.NA, inplace=True) 
        cards_df.replace('NULL',pd.NA, inplace=True)
        cards_df = self.replace_invalid_strings_to_null(cards_df,'card_provider')
        
        # cast card numbers as strings
        cards_df['card_number'] = cards_df['card_number'].astype(str)
        # remove question marks from card numbers
        cards_df['card_number'] = cards_df['card_number'].str.replace('\D+', '', regex=True)
        cards_df.dropna(how='all',subset=['card_provider'], inplace=True)
        return cards_df
        #no duplicate data found in the file   
        #duplicate_cards = cards_df.duplicated(subset=['card_number', 'expiry_date','card_provider'],keep=False)     



    def clean_store_data(self, store_df):
        '''This method cleans data(store data) retrieved through API  and returns dataframe'''
        store_df.replace(r'(<NA>|N/A|NULL)', pd.NA, inplace=True,regex=True)
        store_df['address'].replace(r'\n',',', inplace=True, regex=True)
        store_df.replace(r'^\s*$', pd.NA, regex=True,inplace=True) # replacing blank whitespaces
        store_df.replace(r'[A-Z0-9]{10}', pd.NA, inplace=True,regex=True)
        #taking out the locality from the address as we already have a column which stores locality
        res=store_df['address'].str.rpartition(',')
        store_df['address'] = res[0]
        #drop the column lat as all values in this column are none
        store_df.drop('lat', axis=1, inplace=True)
        # clean incorrect values in continent column
        store_df['continent'] = store_df['continent'].apply(lambda x: x[2:] if x[:2] == 'ee' else x)
        #formatting dates
        store_df = self.clean_date(store_df,'opening_date')
        # clean text from staff_numbers column
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace('[^0-9]', '', regex=True) 
        #find duplicates
        duplicate_stores = store_df.duplicated(subset=['address', 'opening_date','locality'],keep=False)
        #print(store_df[duplicate_stores])
        #drop rows where all the columns in subset are null 
        store_df.dropna(how='all',subset=['address','store_code','locality'],inplace=True)
        #convert the datatype of staff_numbers column to int
        store_df['staff_numbers'] = np.floor(pd.to_numeric(store_df['staff_numbers'], errors='coerce')).astype('Int64')
        # to efficiently use the memory convert following columns to category type
        store_df['continent']= store_df['continent'].astype('category')
        store_df['country_code']= store_df['country_code'].astype('category')
        return store_df
    

    
    def convert_product_weights(self, products_df):
        '''This method is used to clean the weight column from product data and returns dataframe'''
       
        products_df['weight'] = products_df['weight'].fillna('missing')
        products_df['weight'].replace(r'\d+ x \d+', 'missing', inplace= True, regex=True) #reglar expression to replace 'x' symbols 

        products_df['weight'].replace(r'[A-Z0-9]{10}', 'missing', inplace= True, regex=True)
        products_df['weight'].replace(r'77\s\w*', '', inplace= True, regex=True)

        mappings_weight = {'missingg': 'missing'}
        products_df['weight'].replace(mappings_weight, inplace= True)

        def conversion_in_kg(weight):
            '''Method to strip unit strings, convert to float data type, and convert to kilograms.'''
            
            if weight[-2:] == 'kg':
                return float(weight[:-2])
            elif weight.find(' x ') != -1:
                return eval(weight.replace(' x ', '*')[:-1]) / 1000
            elif weight[-1] == 'g' or weight[-2:] == 'ml' or weight.find('.') != -1:
                return float(re.sub('[^0-9]', '', weight)) / 1000
            elif weight[-2:] == 'oz':
                return float(weight[:-2]) * 0.0283495
            else:
                return weight
                        

        products_df['weight'] = products_df['weight'].apply(conversion_in_kg)
        products_df['weight'].replace('missing', np.nan, inplace=True)
    
        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce') # this will replace the values that cannot be converted to numeric with NaN
        products_df['weight'] = products_df['weight'].astype('float64')

        return products_df
    

    def clean_products_data(self, products_df):
        '''This method cleans product data and returns clean dataframe'''
        
        products_df['product_price'] = products_df['product_price'].str.replace('£','')
        products_df=self.replace_invalid_strings_to_null(products_df,'product_price')
        duplicate_products = products_df.duplicated(subset=['product_name', 'weight','category','product_code'],keep=False)
        products_df.dropna(how= 'all', subset=['product_name', 'category','product_code'], inplace=True)
        products_df=self.replace_invalid_strings_to_null(products_df,'category')
        products_df['category']= products_df['category'].astype('category')
        products_df=self.replace_invalid_strings_to_null(products_df,'removed')
        products_df=self.clean_date(products_df,'date_added')
        products_df.drop(products_df[products_df['user_uuid'].str.len() != 36].index, inplace=True)
        return products_df



    def clean_orders_data (self, orders_df):
        orders_df.drop(['level_0', 'index','first_name', 'last_name', '1'], axis=1, inplace=True)
        duplicate_orders = orders_df.duplicated(subset=['date_uuid', 'user_uuid','card_number','product_code'],keep=False)
        # no duplicates present and no null values present
        return orders_df


    def clean_date_times(self, dates_df):
        #formatting timestamp  and repalcing with null for inconsistent values
        dates_df['timestamp']= pd.to_datetime(dates_df['timestamp'], format="%H:%M:%S", errors='coerce') # this adds default date to timestamp
        dates_df['timestamp']= dates_df['timestamp'].dt.time # extracting only timestamp values
        #formatting month, year and day columns
        dates_df = self.clean_date_parts(dates_df,'month')
        dates_df = self.clean_date_parts(dates_df,'year')
        dates_df = self.clean_date_parts(dates_df,'day')
        dates_df= self.replace_invalid_strings_to_null(dates_df,'time_period')
        dates_df.replace('NULL', pd.NA,inplace=True)
        dates_df.drop(dates_df[dates_df['date_uuid'].str.len() != 36].index, inplace=True)
        dates_df.dropna(how = 'all', subset=['timestamp', 'month', 'year', 'day', 'time_period','date_uuid'], inplace=True)
        return dates_df
    
    def clean_date(self,df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], infer_datetime_format=True, errors='coerce')
        return df
    
    def replace_invalid_strings_to_null(self, df,column_name):
        df[column_name].replace(r'[A-Z0-9]{10}', pd.NA, inplace=True, regex=True)
        return df
    
    def clean_date_parts(self,df,column_name):
        df[column_name] = np.round(pd.to_numeric(df[column_name], errors='coerce')) #this function replaces non-numeric values with null
        df[column_name].fillna(0, inplace=True) # filling null values with 0
        df[column_name]= df[column_name].astype(int) # converting month column to int to takeout decimal points
        df[column_name].replace(0,pd.NA,inplace=True)
        return df

