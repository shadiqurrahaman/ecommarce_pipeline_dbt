from pyspark.sql import SparkSession
from configs.etl_base import EtlBase # import all config
import psycopg2

class PostGresSql:

    def __init__(self,client,config_name):
        EtlBase.__init__(self)
        self.db_url = self.appConfig[client][config_name]['url']
        self.db_user = self.appConfig[client][config_name]['username']
        self.db_password = self.appConfig[client][config_name]['password']
        self.driver = self.appConfig[client][config_name]['driver']
        self.db_host = self.appConfig[client][config_name]['host']
        self.db_port = self.appConfig[client][config_name]['port']
        self.db_name = self.appConfig[client][config_name]['database']


    #write data into postgres db using spark
    def save_dataframe_to_postgres(self,data,table_name):
        mode = "overwrite"
        url = self.db_url 
        properties = {"user": self.db_user,"password": self.db_password,"driver": self.driver}
        data.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
    
    #get table data from postgres db (same interface as SqlSever)
    def get_table_data(self,spark,table_name,db_name=None):
        # PostgreSQL URL already includes database name, db_name param is for consistency with SQL Server
        properties = {"user": self.db_user,"password": self.db_password,"driver": self.driver}
        data = spark.read.jdbc(url=self.db_url,table=table_name,properties=properties)
        return data
    

    # read data from postgres db using spark
    def get_data_by_quary(self,spark,_select_sql):
        properties = {"user": self.db_user,"password": self.db_password,"driver": self.driver}
        data = spark.read.jdbc(url=self.db_url,table=_select_sql,properties=properties)
        return data

    
    def execute_postgres_store_procedure(self,_sql_query):
        conn = None
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(user=self.db_user,
                         password= self.db_password,
                         host= self.db_host,
                         port=self.db_port,
                         database=self.db_name)
            # create a cursor object for execution
            cur = conn.cursor()

            # call a stored procedure
            cur.execute('call '+_sql_query+'();')

            # commit the transaction
            conn.commit()

            # close the cursor
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    

