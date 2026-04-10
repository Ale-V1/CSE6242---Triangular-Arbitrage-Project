"""
Configuration file for Binance GCP Loader
Edit these values to match your environment
"""

# GCP Cloud SQL Connection Details
GCP_CONFIG = {
    'host': '34.73.134.13',
    'username': 'postgres',
    'password': 'dva_access',
    'database': 'postgres',  # Change this if you created a different database
    'port': 5432
}

# File Locations
PATHS = {
    # Windows path example
    'parquet_directory': r'/mnt/d/DVA Project/Datasets/Binance Full History',
    
    # Linux/Mac path example (uncomment if needed)
    # 'parquet_directory': '/home/user/binance_data',
}

# Loading Parameters
LOAD_CONFIG = {
    'table_name': 'ohlc_data',
    'chunk_size': 10000,  # Rows per batch insert
    'file_limit': None,   # Set to a number to test with limited files, None to load all
    'file_pattern': '*.parquet'
}

# Database Schema
SCHEMA_CONFIG = {
    'table_name': 'ohlc_data',
    'drop_existing': True,  # WARNING: Set to False to preserve existing data
}
