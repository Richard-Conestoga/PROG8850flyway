"""
NYC 311 ETL Pipeline
Description: Extract NYC 311 service request data, transform, and load into MySQL
"""

import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import sys
import time

# Configuration
CONFIG = {
    'api_url': 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=50000',
    # Alternative: Use smaller limit for testing
    # 'api_url': 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=10000',
    # Or filter by date for incremental loads:
    # 'api_url': 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=50000&$where=created_date>="2024-01-01"',
    'chunk_size': 10000,
    'log_file': 'etl_log.txt',
    'db_config': {
        'host': 'localhost',
        'database': 'nyc311',
        'user': 'root',
        'password': 'Secret5555'
    },
    'table_name': 'service_requests'
}


def setup_logging() -> logging.Logger:
    """Configure logging to both file and console."""
    logger = logging.getLogger('NYC311_ETL')
    logger.setLevel(logging.INFO)
    
    # File handler
    file_handler = logging.FileHandler(CONFIG['log_file'])
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def extract_data(logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Extract data from NYC 311 API with retry logic and streaming.
    
    Returns:
        DataFrame with extracted data or None if extraction fails
    """
    logger.info("Starting data extraction from NYC 311 API...")
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries}: Downloading data...")
            
            # Use streaming to handle large files better
            response = requests.get(
                CONFIG['api_url'], 
                timeout=600,  # Increased to 10 minutes
                stream=True  # Stream the response
            )
            response.raise_for_status()
            
            # Save raw data with progress indication
            temp_file = 'temp_nyc311.csv'
            downloaded_size = 0
            chunk_size = 8192  # 8KB chunks
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        # Log every 5MB
                        if downloaded_size % (5 * 1024 * 1024) < chunk_size:
                            logger.info(f"Downloaded: {downloaded_size / (1024*1024):.1f} MB")
            
            logger.info(f"Download complete. Total size: {downloaded_size / (1024*1024):.1f} MB")
            
            # Read into DataFrame
            logger.info("Parsing CSV data...")
            df = pd.read_csv(temp_file, low_memory=False)
            
            logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"Columns: {', '.join(df.columns.tolist()[:10])}... (showing first 10)")
            
            return df
            
        except requests.exceptions.Timeout as e:
            logger.warning(f"Attempt {attempt} timed out: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("All retry attempts exhausted")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download data from API: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return None
                
        except pd.errors.ParserError as e:
            logger.error(f"Failed to parse CSV data: {str(e)}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            return None
    
    return None


def transform_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Transform and clean the data to match target schema.
    
    Target schema columns:
    - unique_key, created_date, closed_date, agency, complaint_type, 
      descriptor, borough, latitude, longitude
    
    Args:
        df: Raw DataFrame from API
        
    Returns:
        Transformed DataFrame with only target columns
    """
    logger.info("Starting data transformation...")
    
    try:
        # Define target columns based on your schema
        target_columns = [
            'unique_key', 'created_date', 'closed_date', 'agency', 
            'complaint_type', 'descriptor', 'borough', 'latitude', 'longitude'
        ]
        
        # Make a copy and select only needed columns
        df_transformed = df[target_columns].copy()
        
        # Handle missing values
        initial_nulls = df_transformed.isnull().sum().sum()
        logger.info(f"Initial null values: {initial_nulls}")
        
        # Convert date columns to datetime
        date_columns = ['created_date', 'closed_date']
        for col in date_columns:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
            logger.info(f"Converted {col} to datetime")
        
        # Convert unique_key to integer (BIGINT in MySQL)
        df_transformed['unique_key'] = pd.to_numeric(df_transformed['unique_key'], errors='coerce')
        
        # Convert lat/long to numeric
        df_transformed['latitude'] = pd.to_numeric(df_transformed['latitude'], errors='coerce')
        df_transformed['longitude'] = pd.to_numeric(df_transformed['longitude'], errors='coerce')
        
        # Standardize text columns (strip whitespace)
        text_columns = ['agency', 'complaint_type', 'descriptor', 'borough']
        for col in text_columns:
            df_transformed[col] = df_transformed[col].astype(str).str.strip()
            # Replace 'nan' string with None
            df_transformed[col] = df_transformed[col].replace('nan', None)
        
        # Remove rows where unique_key is NULL (can't insert without primary key)
        rows_before = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['unique_key'])
        rows_after = len(df_transformed)
        if rows_before != rows_after:
            logger.warning(f"Removed {rows_before - rows_after} rows with NULL unique_key")
        
        final_nulls = df_transformed.isnull().sum().sum()
        logger.info(f"Final null values: {final_nulls}")
        logger.info(f"Transformation complete. Final shape: {df_transformed.shape}")
        logger.info(f"Null counts by column:\n{df_transformed.isnull().sum()}")
        
        return df_transformed
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise


def create_connection(logger: logging.Logger) -> Optional[mysql.connector.connection.MySQLConnection]:
    """Create database connection with proper authentication handling."""
    try:
        # Remove auth_plugin from config - it's causing issues
        db_config = CONFIG['db_config'].copy()
        
        # Try connection without specifying auth_plugin first
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            db_info = connection.server_info
            logger.info(f"Successfully connected to MySQL Server version {db_info}")
            logger.info(f"Connected to database: {CONFIG['db_config']['database']}")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        logger.error("Troubleshooting tips:")
        logger.error("1. Verify MySQL credentials in CONFIG")
        logger.error("2. Ensure MySQL server is running")
        logger.error("3. Check if database exists")
        logger.error("4. Verify user has proper permissions")
        logger.error("5. If using MySQL 8.0+, the root user may need to use mysql_native_password")
        logger.error("   Run in MySQL: ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Secret';")
        return None



def create_table_if_not_exists(connection, df: pd.DataFrame, logger: logging.Logger) -> bool:
    """
    Verify that the table exists (assumes table already created by DBA/schema script).
    
    In production, table should be created separately via migration scripts.
    This function just validates the table exists.
    """
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_schema = '{CONFIG['db_config']['database']}' 
            AND table_name = '{CONFIG['table_name']}'
        """)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            logger.info(f"Table {CONFIG['table_name']} exists and is ready for loading")
            
            # Get column info for validation
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = '{CONFIG['db_config']['database']}' 
                AND TABLE_NAME = '{CONFIG['table_name']}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = cursor.fetchall()
            logger.info(f"Table has {len(columns)} columns")
            return True
        else:
            logger.error(f"Table {CONFIG['table_name']} does not exist!")
            logger.error("Please create the table first using your schema script")
            return False
        
    except Error as e:
        logger.error(f"Error checking table: {str(e)}")
        return False
    finally:
        cursor.close()


def load_data_in_chunks(connection, df: pd.DataFrame, logger: logging.Logger) -> bool:
    """
    Load data into MySQL in chunks with proper NULL handling.
    Matches schema: unique_key (PK), created_date, closed_date, agency, 
                    complaint_type, descriptor, borough, latitude, longitude
    
    Args:
        connection: MySQL connection object
        df: Transformed DataFrame
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting chunked data load. Total rows: {len(df)}, Chunk size: {CONFIG['chunk_size']}")
    
    try:
        cursor = connection.cursor()
        total_rows = len(df)
        chunks_loaded = 0
        rows_loaded = 0
        
        # Columns must match your table schema exactly
        columns = ['unique_key', 'created_date', 'closed_date', 'agency', 
                   'complaint_type', 'descriptor', 'borough', 'latitude', 'longitude']
        
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Use INSERT IGNORE to skip duplicates (since unique_key is PRIMARY KEY)
        insert_query = f"INSERT IGNORE INTO {CONFIG['table_name']} ({columns_str}) VALUES ({placeholders})"
        
        logger.info(f"Using query: {insert_query}")
        
        # Process data in chunks
        for start_idx in range(0, total_rows, CONFIG['chunk_size']):
            chunk_start_time = time.time()
            end_idx = min(start_idx + CONFIG['chunk_size'], total_rows)
            chunk = df.iloc[start_idx:end_idx]
            
            # Convert DataFrame chunk to list of tuples with proper NULL handling
            data_to_insert = []
            for _, row in chunk[columns].iterrows():
                # Convert each value, replacing NaN/NaT with None
                converted_row = tuple(
                    None if pd.isna(val) else val 
                    for val in row
                )
                data_to_insert.append(converted_row)
            
            # Execute batch insert
            try:
                cursor.executemany(insert_query, data_to_insert)
                rows_inserted = cursor.rowcount
                connection.commit()
                
                chunks_loaded += 1
                rows_loaded += rows_inserted
                chunk_time = time.time() - chunk_start_time
                rows_per_sec = len(chunk) / chunk_time if chunk_time > 0 else 0
                
                logger.info(
                    f"Chunk {chunks_loaded}: Processed rows {start_idx+1} to {end_idx} "
                    f"({len(chunk)} rows, {rows_inserted} inserted) in {chunk_time:.2f}s "
                    f"({rows_per_sec:.0f} rows/sec). Progress: {(end_idx/total_rows)*100:.1f}%"
                )
                
                if rows_inserted < len(chunk):
                    skipped = len(chunk) - rows_inserted
                    logger.warning(f"Skipped {skipped} duplicate rows in chunk {chunks_loaded}")
                
            except Error as e:
                logger.error(f"Error inserting chunk {chunks_loaded + 1}: {str(e)}")
                logger.error(f"First row of failed chunk: {data_to_insert[0] if data_to_insert else 'N/A'}")
                connection.rollback()
                return False
        
        logger.info(f"Successfully loaded {rows_loaded} rows in {chunks_loaded} chunks")
        if rows_loaded < total_rows:
            logger.info(f"Note: {total_rows - rows_loaded} rows were skipped (likely duplicates)")
        
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during load: {str(e)}")
        return False
    finally:
        cursor.close()



def run_etl_pipeline():
    """Main ETL pipeline orchestration."""
    logger = setup_logging()
    logger.info("="*80)
    logger.info("NYC 311 ETL Pipeline Started")
    logger.info("="*80)
    
    start_time = time.time()
    connection = None
    
    try:
        # Extract
        df = extract_data(logger)
        if df is None or df.empty:
            logger.error("Extraction failed or returned empty dataset. Aborting pipeline.")
            return False
        
        # Transform
        df_transformed = transform_data(df, logger)
        
        # Load - Connect to database
        connection = create_connection(logger)
        if connection is None:
            logger.error("Failed to connect to database. Aborting pipeline.")
            return False
        
        # Create table if needed
        if not create_table_if_not_exists(connection, df_transformed, logger):
            logger.error("Failed to create table. Aborting pipeline.")
            return False
        
        # Load data in chunks
        if not load_data_in_chunks(connection, df_transformed, logger):
            logger.error("Failed to load data. Pipeline completed with errors.")
            return False
        
        # Success
        elapsed_time = time.time() - start_time
        logger.info("="*80)
        logger.info(f"ETL Pipeline Completed Successfully in {elapsed_time:.2f} seconds")
        logger.info("="*80)
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {str(e)}")
        return False
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)