import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
#import data_cleaning as dc
#import data_extraction as de
#from data_extraction import DataExtractor

class DatabaseConnector:
    '''This class connects and uploads data to database
    Methods: 

    read_db_creds(self,filename):
        Retrieves database credentials from the YAML file and returns dictionary of credentials
    init_db_engine(self, filename):
        Initialises Postgresql database connection.
    list_db_tables(self,engine):
        Gets the table names of a given database.
    upload_to_db(self,engine,  dataframe, table):
        Uploads pandas DataFrame to SQL database.
    
    '''
    def __init__(self):
        pass
        #self.list_db_tables()

    def read_db_creds(self, filename):
        ''' This method reads the credentials yaml file and return a dictionary of the credentials.
        Args:
            filename:  Yaml file containing credentials
        Returns:
            dictionary'''
        
        self.filename = filename
        with open(self.filename,'r') as f:
            output = yaml.safe_load(f)
        return output
    
    def init_db_engine(self,filename):
        '''This method reads the credentials from the return of read_db_creds and 
        initialise and return an sqlalchemy database engine.
        Args:
            filename with atabase credentials
        Returns:
            sqlalchemy database engine
        '''
        config = self.read_db_creds(filename)
        return create_engine(f"postgresql://{config['RDS_USER']}:{config['RDS_PASSWORD']}@{config['RDS_HOST']}:{config['RDS_PORT']}/{config['RDS_DATABASE']}")
    
    def list_db_tables(self,engine):
        #engine = self.init_db_engine()
        '''This method lists all the tables in the database to extract data from
        Args:
            sqlalchemy database engine
        Returns:
            list of all tables in RDS database'''
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, engine, df,tablename):
        ''' This  method uploads cleaned data to postgreSQL database
        Args:
            engine: sqlalchemy database engine
            df: cleaned dataframe
            tablename: name of the table in database'''
        
        
        df.to_sql(tablename, engine, if_exists='replace')
       






