from pyspark.sql import SparkSession
from configs.etl_base import EtlBase # import all config

class SqlSever:

    def __init__(self,client,config_name):
        EtlBase.__init__(self)
        self.db_url = self.appConfig[client][config_name]['url']
        self.db_user = self.appConfig[client][config_name]['username']
        self.db_password = self.appConfig[client][config_name]['password']
        self.driver = self.appConfig[client][config_name]['driver']
        #self.db_name = self.appConfig[client][config_name]['database']

    #write data into postgres db using spark
    def save_dataframe_with_db(self,data,table_name,db_name):
        #concating jdbcurl and dynamic db name 
        jdbcurl = self.db_url + db_name
        print('sql server...' + jdbcurl)
        data.coalesce(200).write.mode("overwrite").format("jdbc") \
                        .option("url", jdbcurl) \
                        .option("dbtable", table_name) \
                        .option("user", self.db_user) \
                        .option("password", self.db_password) \
                        .option("driver", self.driver) \
                        .save()

    def get_table_data(self,spark,table_name,db_name):
        jbdcurl = self.db_url + db_name
        properties = {"user": self.db_user,"password": self.db_password,"driver": self.driver}
        data = spark.read.jdbc(url=jbdcurl,table=table_name,properties=properties)
        return data

    def append_dataframe_with_db(self, data, table_name, db_name):
        # concating jdbcurl and dynamic db name
        jdbcurl = self.db_url + db_name
        print('sql server...' + jdbcurl)
        data.coalesce(200).write.mode("append").format("jdbc") \
            .option("url", jdbcurl) \
            .option("dbtable", table_name) \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .option("driver", self.driver) \
            .save()