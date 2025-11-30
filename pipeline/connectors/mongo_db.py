from pyspark.sql import SparkSession
from configs.etl_base import EtlBase # import all config
import pymongo
from pymongo import MongoClient
import dns
from datetime import datetime

class MongoDb:

    def __init__(self,client,config_name):
        EtlBase.__init__(self)
        self.connectionString = self.appConfig[client][config_name]['connectionString']
        self.db_name = self.appConfig[client][config_name]['db_name']



    def write_data(self,df,collection_name):        
       df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("overwrite")\
        .option("spark.mongodb.output.uri", self.connectionString)\
        .option("database", self.db_name)\
        .option("collection", collection_name)\
        .save()

    def append_data(self,df,collection_name):        
       df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .mode("append")\
        .option("spark.mongodb.output.uri", self.connectionString)\
        .option("database", self.db_name)\
        .option("collection", collection_name)\
        .save()
 
    def read_data_by_schema(self,spark,df_schema,pipeline,collection_name):    
        data = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .schema(df_schema) \
            .option("spark.mongodb.input.uri", self.connectionString)\
            .option("database",self.db_name)\
            .option("collection", collection_name)\
            .option("pipeline", pipeline)\
            .load()
        return data

    def get_data_by_collection(self,spark,collection_name):    
        data = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("spark.mongodb.input.uri", self.connectionString)\
            .option("database",self.db_name)\
            .option("collection", collection_name)\
            .load()
        return data

    def add_index(self,index_feild,index_name,collection_name):
        connectionString = MongoClient(self.connectionString)
        database = connectionString[self.db_name]
        print('Creating Index in '+  index_feild +' column .... : ')

        database[collection_name].create_index([(index_feild,pymongo.ASCENDING)],name=index_name,background=True)
        print('Successfully!! Created Index in  '+  index_feild +' column .... : ')

    def rename_collection(self,old_collection_name,new_collection_name):
        connectionString = MongoClient(self.connectionString)
        database = connectionString[self.db_name]
        mycollection = database[old_collection_name] 
        mycollection.rename(new_collection_name, dropTarget = True)
        print('Successfully!! rename collection name...')

    def get_last_element_of_collection(self,collection_name,column_name):
        connectionString = MongoClient(self.connectionString)
        database = connectionString[self.db_name]
        mycollection = database[collection_name] 
        last_doc = list(mycollection.find().sort(column_name, -1).limit(1))
        return last_doc

    def insert_document(self,collection_name,document_object):
        connectionString = MongoClient(self.connectionString)
        database = connectionString[self.db_name]
        collection = database[collection_name] 
        rec_id = collection.insert_one(document_object)
        return rec_id

    def get_active_job_list(self,collection_name,jobName=None):
        connectionString = MongoClient(self.connectionString)
        database = connectionString[self.db_name]
        collection = database[collection_name] 
        job_list = list(collection.find(  { "IsActive" : True, "JobName" : jobName}).
        sort('OrderBy', 1))
        return job_list
   

    
