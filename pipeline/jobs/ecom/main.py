from pipeline.common.spark import start_spark
import os
from pipeline.configs.etl_base import EtlBase
from pipeline.jobs.ecom.extract.data_extractor import DataExtractor
import json

class ETL:
    def __init__(self) -> None:
        EtlBase.__init__(self)
        
        # Get JDBC JARs from project
        jdbc_jars = '/home/sh/Desktop/ecom/pipeline/spark_jars/mssql-jdbc-8.2.0.jre8.jar'
        
        spark,logs,config_dict = start_spark(
            app_name='my_etl_job',
            spark_config={
                'spark.jars': jdbc_jars,
                'spark.executor.heartbeatInterval': '100000ms',
                'spark.driver.memory': '5G',
                'spark.executor.memory': '10G',
                'spark.sql.debug.maxToStringFields': '200'
            }
        )
        self.tables_config = self.appConfig['datasource']['sqlserver_local']['tables']
        self.extract(spark, self.tables_config)
        
    def extract(self, spark, tables_config):
        """Extract data from source database using the enhanced DataExtractor"""
        # Initialize extractor
        extractor = DataExtractor(client='datasource')
        
        # Extract all tables from SQL Server
        # The extractor will automatically chunk large tables
        results = extractor.extract_tables(
            spark=spark,
            tables_config=tables_config,
            config_name='sqlserver_local',  # Can be changed to 'postgres_local' or any other config
            skip_on_error=True  # Continue even if some tables fail
        )
        
        # Process results
        print("\n" + "="*80)
        print("EXTRACTION SUMMARY")
        print("="*80)
        
        for table_name, (df, metadata) in results.items():
            if df is not None:
                print(f"\n✓ {table_name}")
                print(f"  Rows: {metadata.get('extracted_rows', 0):,}")
                print(f"  Chunked: {metadata.get('chunked', False)}")
                
                if metadata.get('chunked'):
                    print(f"  Chunks: {metadata.get('num_chunks', 0)}")
                    print(f"  Chunk size: {metadata.get('chunk_size', 0):,} rows")
                
                # Show sample data
                print(f"\n  Sample data:")
                df.show(5, truncate=False)
                
            else:
                print(f"\n✗ {table_name} - FAILED")
                print(f"  Error: {metadata.get('error', 'Unknown error')}")
        
        print("\n" + "="*80)
        
        # Return results for transform/load stages
        return results


if __name__=='__main__':
    ETL()
