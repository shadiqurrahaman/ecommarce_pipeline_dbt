"""
Enhanced Data Extractor with automatic chunking and pagination support for large tables.
Supports multiple database types (SQL Server, PostgreSQL) with config-driven extraction.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, List, Optional, Tuple
import logging
from configs.etl_base import EtlBase
from connectors.sql_server import SqlSever
from connectors.postgres_sql import PostGresSql


class DataExtractor:
    """
    Standalone extractor class that handles data extraction from multiple database types
    with automatic chunking for large tables.
    """
    
    # Configuration for auto-chunking
    DEFAULT_CHUNK_SIZE = 100000  # Rows per chunk
    SIZE_THRESHOLD_MB = 50  # Auto-chunk if table > 50MB
    
    def __init__(self, client: str = 'datasource'):
        """
        Initialize the data extractor.
        
        Args:
            client: Configuration client name (default: 'datasource')
        """
        EtlBase.__init__(self)
        self.client = client
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging for the extractor."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _get_connector(self, config_name: str):
        """
        Get the appropriate database connector based on config name.
        
        Args:
            config_name: Configuration name ('sqlserver_local', 'postgres_local', etc.)
            
        Returns:
            Database connector instance
        """
        config_name_lower = config_name.lower()
        
        if 'sqlserver' in config_name_lower or 'mssql' in config_name_lower:
            return SqlSever(self.client, config_name)
        elif 'postgres' in config_name_lower or 'postgresql' in config_name_lower:
            return PostGresSql(self.client, config_name)
        else:
            raise ValueError(
                f"Unsupported database type for config: {config_name}. "
                f"Supported types: sqlserver, postgres"
            )
    
    def _get_table_size(
        self, 
        spark: SparkSession, 
        connector, 
        table_name: str,
        schema_name: str, 
        db_name: str
    ) -> Tuple[int, float]:
        """
        Get the row count and estimated size of a table.
        
        Args:
            spark: Spark session
            connector: Database connector
            table_name: Name of the table
            schema_name: Schema name
            db_name: Database name
            
        Returns:
            Tuple of (row_count, estimated_size_mb)
        """
        try:
            # Build fully qualified table name with SQL Server bracket notation
            if schema_name:
                fully_qualified_table = f"[{schema_name}].[{table_name}]"
            else:
                fully_qualified_table = f"[{table_name}]"
            
            # Build count query - wrap in subquery for JDBC compatibility
            count_query = f"(SELECT COUNT(*) as cnt FROM {fully_qualified_table}) as count_table"
            
            # Execute count query
            count_df = connector.get_table_data(spark, count_query, db_name)
            row_count = count_df.first()['cnt']
            
            # Estimate size (rough estimate: 1KB per row average)
            estimated_size_mb = (row_count * 1024) / (1024 * 1024)
            
            self.logger.debug(
                f"Table {schema_name}.{table_name}: {row_count:,} rows, "
                f"~{estimated_size_mb:.2f} MB"
            )
            
            return row_count, estimated_size_mb
            
        except Exception as e:
            self.logger.warning(
                f"Could not determine size for {schema_name}.{table_name}: {e}. "
                f"Will attempt full extraction."
            )
            return 0, 0
    
    def _get_primary_key(
        self, 
        spark: SparkSession, 
        connector, 
        table_name: str,
        schema_name: str, 
        db_name: str,
        config_name: str
    ) -> Optional[str]:
        """
        Try to detect the primary key column for a table.
        
        Args:
            spark: Spark session
            connector: Database connector
            table_name: Name of the table
            schema_name: Schema name
            db_name: Database name
            config_name: Configuration name to determine database type
            
        Returns:
            Primary key column name or None
        """
        try:
            config_name_lower = config_name.lower()
            
            if 'sqlserver' in config_name_lower:
                # SQL Server primary key detection
                pk_query = f"""
                (SELECT c.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
                    ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = '{table_name}'
                    AND tc.TABLE_SCHEMA = '{schema_name}') as pk_table
                """
            elif 'postgres' in config_name_lower:
                # PostgreSQL primary key detection (no brackets for PostgreSQL)
                fully_qualified = f"{schema_name}.{table_name}" if schema_name else table_name
                pk_query = f"""
                (SELECT a.attname as COLUMN_NAME
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{fully_qualified}'::regclass
                    AND i.indisprimary) as pk_table
                """
            else:
                return None
            
            pk_df = connector.get_table_data(spark, pk_query, db_name)
            pk_rows = pk_df.collect()
            
            if pk_rows and len(pk_rows) > 0:
                pk_column = pk_rows[0]['COLUMN_NAME']
                self.logger.debug(f"Detected primary key for {schema_name}.{table_name}: {pk_column}")
                return pk_column
            
        except Exception as e:
            self.logger.debug(f"Could not detect primary key for {schema_name}.{table_name}: {e}")
        
        return None
    
    def _extract_with_chunks(
        self,
        spark: SparkSession,
        connector,
        table_name: str,
        schema_name: str,
        db_name: str,
        row_count: int,
        chunk_size: int,
        primary_key: Optional[str] = None
    ) -> Tuple[DataFrame, Dict]:
        """
        Extract table data in chunks and union them.
        
        Args:
            spark: Spark session
            connector: Database connector
            table_name: Name of the table
            schema_name: Schema name
            db_name: Database name
            row_count: Total number of rows
            chunk_size: Rows per chunk
            primary_key: Primary key column for ordering (optional)
            
        Returns:
            Tuple of (combined DataFrame, metadata dict)
        """
        num_chunks = (row_count + chunk_size - 1) // chunk_size
        
        # Build fully qualified table name with SQL Server bracket notation
        if schema_name:
            fully_qualified_table = f"[{schema_name}].[{table_name}]"
        else:
            fully_qualified_table = f"[{table_name}]"
        
        self.logger.debug(
            f"Extracting {fully_qualified_table} in {num_chunks} chunks "
            f"of {chunk_size:,} rows each"
        )
        
        chunks = []
        metadata = {
            'table_name': table_name,
            'total_rows': row_count,
            'chunk_size': chunk_size,
            'num_chunks': num_chunks,
            'chunks_info': []
        }
        
        for i in range(num_chunks):
            offset = i * chunk_size
            
            try:
                # Build query with OFFSET/FETCH (SQL Server 2012+) or LIMIT/OFFSET (PostgreSQL)
                if primary_key:
                    # Use primary key for consistent ordering
                    chunk_query = f"""
                    (SELECT * FROM {fully_qualified_table} 
                     ORDER BY {primary_key}
                     OFFSET {offset} ROWS 
                     FETCH NEXT {chunk_size} ROWS ONLY) as chunk_{i}
                    """
                else:
                    # Fallback without specific ordering (may be inconsistent)
                    chunk_query = f"""
                    (SELECT * FROM {fully_qualified_table}
                     OFFSET {offset} ROWS 
                     FETCH NEXT {chunk_size} ROWS ONLY) as chunk_{i}
                    """
                
                chunk_df = connector.get_table_data(spark, chunk_query, db_name)
                chunk_row_count = chunk_df.count()
                
                # Add chunk metadata
                chunk_df = chunk_df.withColumn('_chunk_id', lit(i))
                chunks.append(chunk_df)
                
                metadata['chunks_info'].append({
                    'chunk_id': i,
                    'offset': offset,
                    'rows': chunk_row_count
                })
                
                self.logger.debug(
                    f"Chunk {i + 1}/{num_chunks}: "
                    f"Extracted {chunk_row_count:,} rows (offset: {offset:,})"
                )
                
            except Exception as e:
                self.logger.error(
                    f"Failed to extract chunk {i + 1}/{num_chunks} "
                    f"for {table_name}: {e}"
                )
                metadata['chunks_info'].append({
                    'chunk_id': i,
                    'offset': offset,
                    'error': str(e)
                })
        
        # Union all chunks
        if chunks:
            combined_df = chunks[0]
            for chunk_df in chunks[1:]:
                combined_df = combined_df.union(chunk_df)
            
            metadata['extracted_rows'] = combined_df.count()
            return combined_df, metadata
        else:
            raise Exception(f"No chunks successfully extracted for {table_name}")
    
    def extract_table(
        self,
        spark: SparkSession,
        table_name: str,
        config_name: str,
        schema_name: Optional[str] = None,
        db_name: Optional[str] = None,
        chunk_size: Optional[int] = None,
        force_chunking: bool = False
    ) -> Tuple[DataFrame, Dict]:
        """
        Extract a table from database with automatic chunking for large tables.
        
        Args:
            spark: Spark session
            table_name: Name of the table to extract
            config_name: Configuration name ('sqlserver_local', 'postgres_local')
            schema_name: Schema name (optional, defaults to config or 'raw')
            db_name: Database name (optional, will use config default)
            chunk_size: Custom chunk size in rows (optional, uses DEFAULT_CHUNK_SIZE)
            force_chunking: Force chunking even for small tables
            
        Returns:
            Tuple of (DataFrame, metadata dict with partition info)
            
        Raises:
            Exception: If extraction fails
        """
        try:
            # Get connector
            connector = self._get_connector(config_name)
            
            # Get database name from config if not provided
            if db_name is None:
                db_name = self.appConfig[self.client][config_name].get('database', '')
            
            # Get schema name - default to 'raw' if not provided
            if schema_name is None:
                schema_name = 'raw'
            
            # Determine chunk size
            chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
            
            # Get table size
            row_count, size_mb = self._get_table_size(
                spark, connector, table_name, schema_name, db_name
            )
            
            # Decide whether to chunk
            should_chunk = (
                force_chunking or 
                size_mb > self.SIZE_THRESHOLD_MB or
                row_count > chunk_size
            )
            
            metadata = {
                'table_name': table_name,
                'schema_name': schema_name,
                'config_name': config_name,
                'database': db_name,
                'row_count': row_count,
                'estimated_size_mb': size_mb,
                'chunked': should_chunk
            }
            
            if should_chunk and row_count > 0:
                # Try to detect primary key for consistent ordering
                primary_key = self._get_primary_key(
                    spark, connector, table_name, schema_name, db_name, config_name
                )
                
                if not primary_key:
                    self.logger.warning(
                        f"No primary key detected for {table_name}. "
                        f"Chunking may be inconsistent."
                    )
                
                # Extract with chunks
                df, chunk_metadata = self._extract_with_chunks(
                    spark, connector, table_name, schema_name, db_name, 
                    row_count, chunk_size, primary_key
                )
                metadata.update(chunk_metadata)
                
            else:
                # Extract full table in one go
                # Build fully qualified table name with SQL Server bracket notation
                if schema_name:
                    fully_qualified_table = f"[{schema_name}].[{table_name}]"
                else:
                    fully_qualified_table = f"[{table_name}]"
                
                # Wrap in subquery for JDBC compatibility with bracketed names
                table_query = f"(SELECT * FROM {fully_qualified_table}) as {table_name}_data"
                
                self.logger.debug(f"Extracting {fully_qualified_table} in single operation")
                df = connector.get_table_data(spark, table_query, db_name)
                metadata['extracted_rows'] = df.count()
                metadata['chunks_info'] = []
            
            # self.logger.info(
            #     f"Successfully extracted {table_name}: "
            #     f"{metadata['extracted_rows']:,} rows"
            # )
            
            return df, metadata
            
        except Exception as e:
            error_msg = f"Failed to extract {table_name} from {config_name}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
    
    def extract_tables(
        self,
        spark: SparkSession,
        tables_config,
        config_name: str,
        db_name: Optional[str] = None,
        chunk_size: Optional[int] = None,
        skip_on_error: bool = True
    ) -> Dict[str, Tuple[Optional[DataFrame], Dict]]:
        """
        Extract multiple tables from database.
        
        Args:
            spark: Spark session
            tables_config: Either a list of table names or dictionary with table configs
                          List format: ['Table1', 'Table2', ...]
                          Dict format: {'Table1': {'schema': 'raw'}, 'Table2': {'schema': 'sales'}, ...}
            config_name: Configuration name ('sqlserver_local', 'postgres_local')
            db_name: Database name (optional, will use config default)
            chunk_size: Custom chunk size in rows (optional)
            skip_on_error: If True, skip failed tables and continue; if False, raise exception
            
        Returns:
            Dictionary mapping table_name -> (DataFrame or None, metadata dict)
            If a table fails and skip_on_error=True, DataFrame will be None
        """
        results = {}
        
        # Handle both list and dict formats
        if isinstance(tables_config, list):
            # Convert list to dict format with default schema
            tables_dict = {table_name: {'schema': 'raw'} for table_name in tables_config}
        elif isinstance(tables_config, dict):
            tables_dict = tables_config
        else:
            raise ValueError(
                f"tables_config must be either list or dict, got {type(tables_config)}"
            )
        
        # self.logger.info(
        #     f"Starting extraction of {len(tables_dict)} tables from {config_name}"
        # )
        
        for table_name, table_config in tables_dict.items():
            try:
                # Get schema from config, default to 'raw'
                schema_name = table_config.get('schema', 'raw') if isinstance(table_config, dict) else 'raw'
                
                # self.logger.debug(
                #     f"Extracting table: {table_name}, schema: {schema_name}, "
                #     f"config: {table_config}"
                # )
                
                df, metadata = self.extract_table(
                    spark, table_name, config_name, schema_name, db_name, chunk_size
                )
                results[table_name] = (df, metadata)
                
            except Exception as e:
                self.logger.error(f"Error extracting {table_name}: {e}")
                
                if skip_on_error:
                    # Log error and continue
                    results[table_name] = (None, {
                        'table_name': table_name,
                        'config_name': config_name,
                        'error': str(e),
                        'status': 'failed'
                    })
                else:
                    # Re-raise exception
                    raise
        
        # Summary
        successful = sum(1 for df, _ in results.values() if df is not None)
        failed = len(results) - successful
        
        self.logger.info(
            f"Extraction complete: {successful} succeeded, {failed} failed"
        )
        
        return results
