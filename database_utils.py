import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
#import data_cleaning as dc
#import data_extraction as de
#from data_extraction import DataExtractor

class DatabaseConnector:
    def __init__(self):
        pass
        #self.list_db_tables()

    def read_db_creds(self, filename):
        self.filename = filename
        with open(self.filename,'r') as f:
            output = yaml.safe_load(f)
        return output
    
    def init_db_engine(self,filename):
        config = self.read_db_creds(filename)
        return create_engine(f"postgresql://{config['RDS_USER']}:{config['RDS_PASSWORD']}@{config['RDS_HOST']}:{config['RDS_PORT']}/{config['RDS_DATABASE']}")
    
    def list_db_tables(self,engine):
        #engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, engine, df,tablename):
      
        #df.to_sql('dim_users', engine)
        #df.to_sql('dim_card_details', engine)
        df.to_sql(tablename, engine, if_exists='replace')
        #print(dfusers)






