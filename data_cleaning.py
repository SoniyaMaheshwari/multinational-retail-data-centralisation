
import database_utils as db
import data_extraction as de
import pandas as pd
import numpy as np
import re

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self,dfusers):
        #dfusers = dataex.read_rds_table(dbcon)

        dfusers['country_code'] = dfusers['country_code'].astype('string')
        mapping = {'GGB':'GB','PG8MOC0UZI':'','0CU6LW3NKB':'', 'QVUW9JSKY3':'', 'VSM4IZ4EL3':'', 'VSM4IZ4EL3':'' ,'NTCGYW8LVC':'','RVRFD92E48':'','XKI9UXSCZ1':'', 'QREF9WLI2A':'', 'XPVCZE2L8B':'','44YAIDY048':'', 'FB13AKRI21':'','OS2P9CMHR6':'','5D74J6FPFJ':'','LZGTB0T5Z7':'','IM8MN1L9MJ':''}
        # cleaning country code
        dfusers['country_code'] = dfusers['country_code'].replace(mapping)
        dfusers['country_code'] = dfusers['country_code'].replace('', pd.NA)
        dfusers.replace('NULL', pd.NA, inplace=True)
        dfusers.dropna(axis='index',how = 'all', subset=['first_name','last_name'], inplace=True)
        # cleaning phone number
        dfusers['phone_number'] = dfusers['phone_number'].str.replace('(0)', '')
        dfusers['phone_number'] = dfusers['phone_number'].str.replace('\W', '', regex=True)
        dfusers['phone_number'] = dfusers['phone_number'].str.replace('x', '')
        dfusers['phone_number'] = dfusers['phone_number']. astype('string')
        dfusers['phone_number']=dfusers['phone_number'].apply(lambda x: x if re.findall(r'00\d+', x) else "00" + x  )
        dfusers['phone_number'].replace('',pd.NA, inplace=True)
        # cleaning dates columns
        dfusers["join_date"] = pd.to_datetime(dfusers["join_date"], infer_datetime_format=True, errors='coerce')
        dfusers["date_of_birth"] = pd.to_datetime(dfusers["date_of_birth"], infer_datetime_format=True, errors='coerce') 
        #dfusers.drop('level_0', axis=1, inplace=True)
        #print(dfusers)
        return dfusers  

    def clean_card_details(self, cards_df):
        cards_df.replace('',pd.NA, inplace=True) 
        cards_df.replace('NULL',pd.NA, inplace=True)
        cards_df['card_provider'].replace(r'[A-Z0-9]{10}', pd.NA, inplace=True,regex=True)
        cards_df.dropna(how='all',subset=['card_provider'], inplace=True)
        return cards_df
        #no duplicate data found in the file   
        #duplicate_cards = cards_df.duplicated(subset=['card_number', 'expiry_date','card_provider'],keep=False)     



    def clean_store_data(self, store_df):
        store_df.replace(r'(<NA>|N/A|NULL)', pd.NA, inplace=True,regex=True)
        store_df['address'].replace(r'\n',',', inplace=True, regex=True)
        store_df.replace(r'^\s*$', pd.NA, regex=True,inplace=True) # replacing blank whitespaces
        store_df.replace(r'[A-Z0-9]{10}', pd.NA, inplace=True,regex=True)

        #taking out the locality from the address as we already have a column which stores locality
        res=store_df['address'].str.rpartition(',')
        store_df['address'] = res[0]

        #drop the column lat as all values in this column are none
        store_df.drop('lat', axis=1, inplace=True)

        #deleting row at index 447 as it has meaningless values
        store_df=store_df[store_df.index != 447]

        cont_mapping={'eeEurope': 'Europe', 'eeAmerica':'America', 'NULL':pd.NA}
        store_df['continent'].replace(cont_mapping,inplace=True)

        #formatting dates
        store_df["opening_date"] = pd.to_datetime(store_df["opening_date"], errors='coerce') 

        #replace non-numeric values from staff numbers with numeric values
        mapping_staff_num={'J78': 78, '30e':30,'80R':80,'A97':97,'3n9':39}
        store_df['staff_numbers'].replace(mapping_staff_num,inplace=True)

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

        #store_df.drop('level_0', axis=1, inplace=True)

        return store_df
    

    
    def convert_product_weights(self, products_df):
        products_df['weight'] = products_df['weight'].fillna('missing')
        products_df['weight'].replace(r'\d+ x \d+', 'missing', inplace= True, regex=True)

        products_df['weight'].replace(r'[A-Z0-9]{10}', 'missing', inplace= True, regex=True)
        products_df['weight'].replace(r'77\s\w*', '', inplace= True, regex=True)

        mappings_weight = {'77g .': '77', 'missingg': 'missing', '100ml': '100g','60ml':'60g', '300ml': '300g', '':'missing', '16oz':'16g', '800ml': '800g'}
        products_df['weight'].replace(mappings_weight, inplace= True)

        def conversion_in_kg(x):
            if x == 'missing':
                return x
            else:
                if 'kg' in x:
                    x= x.replace('kg', '')
                    return float(x)
                else:
                    x = x.replace('g', '')
                    x = float(x) /1000
                    return float(x)
                        

        products_df['weight'] = products_df['weight'].apply(conversion_in_kg)
        products_df['weight'].replace('missing', np.nan, inplace=True)
    
        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce') # this will replace the values that cannot be converted to numeric with NaN
        products_df['weight'] = products_df['weight'].astype('float64')

        return products_df
    

    def clean_products_data(self, products_df):
        products_df['product_price'] = products_df['product_price'].str.replace('£','')
        products_df['product_price'].replace(r'[A-Z0-9]{10}', pd.NA, inplace= True, regex=True)
        duplicate_products = products_df.duplicated(subset=['product_name', 'weight','category','product_code'],keep=False)
        products_df.dropna(how= 'all', subset=['product_name', 'category','product_code'], inplace=True)
        products_df['category'].replace(r'[A-Z0-9]{10}', pd.NA, inplace=True, regex=True)
        products_df['category']= products_df['category'].astype('category')
        products_df['removed'].replace(r'[A-Z0-9]{10}', pd.NA, inplace=True, regex=True)
        products_df["date_added"] = pd.to_datetime(products_df["date_added"], errors='coerce') 

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
        
        dates_df['month'] = np.round(pd.to_numeric(dates_df['month'], errors='coerce')) #this function replaces non-numeric values with null
        dates_df['month'].fillna(0, inplace=True) # filling null values with 0
        dates_df['month']= dates_df['month'].astype(int) # converting month column to int to takeout decimal points
        dates_df['month'].replace(0,pd.NA,inplace=True)

        dates_df['year'] = np.round(pd.to_numeric(dates_df['year'], errors='coerce'))
        dates_df['year'].fillna(0, inplace=True)
        dates_df['year']= dates_df['year'].astype(int)
        dates_df['year'].replace(0,pd.NA,inplace=True)

        dates_df['day'] = np.round(pd.to_numeric(dates_df['day'], errors='coerce'))
        dates_df['day'].fillna(0, inplace=True)
        dates_df['day']= dates_df['day'].astype(int)
        dates_df['day'].replace(0,pd.NA,inplace=True)

        dates_df['time_period'].replace(r'[A-Z0-9]{10}', pd.NA, inplace=True, regex=True)
        dates_df.replace('NULL', pd.NA,inplace=True)

        dates_df.dropna(how = 'all', subset=['timestamp', 'month', 'year', 'day', 'time_period','date_uuid'], inplace=True)

        return dates_df