from configs.etl_base import EtlBase # import all config
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import datetime

class BlobReader:

    def __init__(self,client, connection_config):
        EtlBase.__init__(self)
        self.connection_config = connection_config
        self.storage_connection_str=self.appConfig[client][connection_config]["storageConnectionString"]
        self.storage_container_name=self.appConfig[client][connection_config]["containerName"]

    def get_blob_list(self,blob_directory_path=None):
        
        # BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.storage_connection_str)        
        # create container client
        container_client = blob_service_client.get_container_client(self.storage_container_name)
        # lists all the blobs 
        blob_list = container_client.list_blobs(name_starts_with=blob_directory_path)
       
        return blob_list


    def read_blob(self,blob_name):
        blob=BlobClient.from_connection_string(
                    conn_str=self.storage_connection_str,
                    container_name=self.storage_container_name,
                    blob_name=blob_name)
        return blob



    def dowlload_blob(self,blob_client, destination_file,file_name):
        print("[{}]:[INFO] : Downloading {} ...".format(datetime.datetime.utcnow(),destination_file+file_name))
        with open(destination_file+file_name, "wb") as my_blob:
            blob_data = blob_client.download_blob()
            blob_data.readinto(my_blob)       
        print("[{}]:[INFO] : download finished".format(datetime.datetime.utcnow()))

    
    
    