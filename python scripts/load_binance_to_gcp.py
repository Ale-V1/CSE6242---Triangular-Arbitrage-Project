#!/usr/bin/env python3
"""
Load Binance parquet files into GCP Cloud SQL PostgreSQL database
Automatically parses base and quote currencies from filenames
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os
from datetime import datetime

class BinanceGCPLoader:
    def __init__(self, host, username, password, database='postgres', port=5432):
        """
        Initialize GCP Cloud SQL connection
        
        Args:
            host: GCP Cloud SQL Public IP
            username: Database username
            password: Database password
            database: Database name (default: postgres)
            port: Port number (default: 5432)
        """
        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(db_url, pool_pre_ping=True)
        print(f"Connected to GCP Cloud SQL: {host}")
        
    def create_ohlc_table(self, table_name='ohlc_data'):
        """
        Create the OHLC table with enhanced schema including:
        - id (bigserial primary key)
        - load_time (timestamp with default)
        - base_currency (parsed from filename)
        - quote_currency (parsed from filename)
        """
        create_table_sql = f"""
        DROP TABLE IF EXISTS {table_name} CASCADE;
        
        CREATE TABLE {table_name} (
            id BIGSERIAL PRIMARY KEY,
            pair VARCHAR(20) NOT NULL,
            base_currency VARCHAR(10) NOT NULL,
            quote_currency VARCHAR(10) NOT NULL,
            open_time TIMESTAMP NOT NULL,
            open DECIMAL(20, 10) NOT NULL,
            high DECIMAL(20, 10) NOT NULL,
            low DECIMAL(20, 10) NOT NULL,
            close DECIMAL(20, 10) NOT NULL,
            volume DECIMAL(20, 10) NOT NULL,
            quote_asset_volume DECIMAL(20, 10) NOT NULL,
            number_of_trades INTEGER NOT NULL,
            taker_buy_base_asset_volume DECIMAL(20, 10) NOT NULL,
            taker_buy_quote_asset_volume DECIMAL(20, 10) NOT NULL,
            load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(pair, open_time)
        );
        
        -- Create indexes for efficient queries
        CREATE INDEX idx_{table_name}_open_time ON {table_name}(open_time);
        CREATE INDEX idx_{table_name}_pair ON {table_name}(pair);
        CREATE INDEX idx_{table_name}_base_curr ON {table_name}(base_currency);
        CREATE INDEX idx_{table_name}_quote_curr ON {table_name}(quote_currency);
        CREATE INDEX idx_{table_name}_pair_time ON {table_name}(pair, open_time);
        
        -- Index for triangular arbitrage queries (3-way joins)
        CREATE INDEX idx_{table_name}_currencies ON {table_name}(base_currency, quote_currency, open_time);
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        print(f"✓ Table '{table_name}' created with enhanced schema")
        print(f"  Columns: id, pair, base_currency, quote_currency, open_time, OHLC data, load_time")
    
    def parse_pair(self, filename):
        """
        Parse base and quote currencies from filename
        
        Args:
            filename: e.g., "1INCH-BTC.parquet" or "BTC-USDT.parquet"
            
        Returns:
            tuple: (pair_name, base_currency, quote_currency)
            e.g., ("1INCH-BTC", "1INCH", "BTC")
        """
        # Remove .parquet extension
        pair_name = Path(filename).stem
        
        # Split on dash to get base and quote
        if '-' in pair_name:
            parts = pair_name.split('-')
            base_currency = parts[0]
            quote_currency = parts[1]
        else:
            raise ValueError(f"Invalid filename format: {filename}. Expected format: BASE-QUOTE.parquet")
        
        return pair_name, base_currency, quote_currency
    
    def load_parquet_file(self, parquet_file, table_name='ohlc_data', chunk_size=10000):
        """
        Load a single parquet file into PostgreSQL
        
        Args:
            parquet_file: Path to parquet file
            table_name: Target table name
            chunk_size: Number of rows to insert at once
        """
        try:
            # Parse filename to get pair and currencies
            pair_name, base_currency, quote_currency = self.parse_pair(parquet_file)
            
            print(f"\nLoading {pair_name} ({base_currency}/{quote_currency})...")
            
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            
            # Add metadata columns
            df['pair'] = pair_name
            df['base_currency'] = base_currency
            df['quote_currency'] = quote_currency
            
            # Reset index to make open_time a column
            df = df.reset_index()
            
            # Reorder columns to match table schema (excluding id and load_time which are auto-generated)
            columns = [
                'pair', 'base_currency', 'quote_currency', 'open_time',
                'open', 'high', 'low', 'close', 
                'volume', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
            ]
            df = df[columns]
            
            total_rows = len(df)
            print(f"  Rows to insert: {total_rows:,}")
            
            # Insert in chunks for better performance
            rows_inserted = 0
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(table_name, self.engine, if_exists='append', 
                           index=False, method='multi')
                
                rows_inserted += len(chunk)
                progress = min(i + chunk_size, total_rows)
                print(f"  Progress: {progress:,}/{total_rows:,} ({100*progress/total_rows:.1f}%)", end='\r')
            
            print(f"\n  ✓ Completed {pair_name}: {rows_inserted:,} rows inserted")
            return True
            
        except Exception as e:
            print(f"\n  ✗ Error loading {Path(parquet_file).name}: {e}")
            return False
    
    def load_directory(self, directory, table_name='ohlc_data', 
                      chunk_size=10000, pattern='*.parquet', limit=None):
        """
        Load all parquet files from a directory
        
        Args:
            directory: Directory containing parquet files
            table_name: Target table name
            chunk_size: Rows per insert batch
            pattern: File pattern to match (default: *.parquet)
            limit: Maximum number of files to load (None for all)
        """
        parquet_files = sorted(list(Path(directory).glob(pattern)))
        
        if limit:
            parquet_files = parquet_files[:limit]
        
        total_files = len(parquet_files)
        print(f"\n{'='*60}")
        print(f"Found {total_files} parquet files to load")
        print(f"{'='*60}")
        
        success_count = 0
        fail_count = 0
        start_time = datetime.now()
        
        for i, file in enumerate(parquet_files, 1):
            print(f"\n[{i}/{total_files}]", end=" ")
            if self.load_parquet_file(file, table_name, chunk_size):
                success_count += 1
            else:
                fail_count += 1
        
        elapsed = datetime.now() - start_time
        
        print(f"\n{'='*60}")
        print(f"Load Complete!")
        print(f"{'='*60}")
        print(f"  ✓ Successful: {success_count}/{total_files}")
        print(f"  ✗ Failed: {fail_count}/{total_files}")
        print(f"  Time elapsed: {elapsed}")
        print(f"{'='*60}")
    
    def get_table_stats(self, table_name='ohlc_data'):
        """
        Get statistics about loaded data
        """
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT pair) as unique_pairs,
            COUNT(DISTINCT base_currency) as unique_base,
            COUNT(DISTINCT quote_currency) as unique_quote,
            MIN(open_time) as earliest_date,
            MAX(open_time) as latest_date,
            MIN(load_time) as first_load,
            MAX(load_time) as last_load
        FROM {table_name};
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(stats_sql))
            stats = result.fetchone()
        
        print(f"\n{'='*60}")
        print(f"Database Statistics - {table_name}")
        print(f"{'='*60}")
        print(f"  Total rows: {stats[0]:,}")
        print(f"  Unique pairs: {stats[1]:,}")
        print(f"  Unique base currencies: {stats[2]:,}")
        print(f"  Unique quote currencies: {stats[3]:,}")
        print(f"  Date range: {stats[4]} to {stats[5]}")
        print(f"  Load time range: {stats[6]} to {stats[7]}")
        print(f"{'='*60}")
        
        # Show top pairs by volume
        volume_sql = f"""
        SELECT 
            pair,
            base_currency,
            quote_currency,
            COUNT(*) as candles,
            SUM(volume) as total_volume,
            AVG(volume) as avg_volume
        FROM {table_name}
        GROUP BY pair, base_currency, quote_currency
        ORDER BY total_volume DESC
        LIMIT 10;
        """
        
        print(f"\nTop 10 Pairs by Volume:")
        print(f"{'-'*60}")
        with self.engine.connect() as conn:
            result = conn.execute(text(volume_sql))
            for row in result:
                print(f"  {row[0]:15} {row[1]:10}/{row[2]:10} {row[3]:8,} candles  Vol: {row[4]:,.2f}")
    
    def sample_triangular_arbitrage_query(self, table_name='ohlc_data'):
        """
        Example query for finding triangular arbitrage opportunities
        """
        print(f"\n{'='*60}")
        print("Sample Triangular Arbitrage Query")
        print(f"{'='*60}")
        
        arb_sql = f"""
        SELECT 
            a.open_time,
            a.pair as pair_1,
            b.pair as pair_2,
            c.pair as pair_3,
            a.close as price_1,
            b.close as price_2,
            c.close as price_3,
            (a.close * b.close) / c.close - 1 as price_diff_pct,
            a.volume as vol_1,
            b.volume as vol_2,
            c.volume as vol_3
        FROM {table_name} a
        JOIN {table_name} b ON a.open_time = b.open_time
        JOIN {table_name} c ON a.open_time = c.open_time
        WHERE a.pair = 'BTC-ETH' 
          AND b.pair = 'ETH-USDT'
          AND c.pair = 'BTC-USDT'
          AND a.open_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'
          AND ABS((a.close * b.close) / c.close - 1) > 0.001
        ORDER BY ABS(price_diff_pct) DESC
        LIMIT 5;
        """
        
        print("Query: Find BTC-ETH-USDT triangular price differences > 0.1%")
        print(f"{'-'*60}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(arb_sql))
                rows = result.fetchall()
                
                if rows:
                    for row in rows:
                        print(f"  {row[0]} | {row[1]} x {row[2]} / {row[3]} = {row[7]*100:.3f}% diff")
                else:
                    print("  No opportunities found in the last 7 days")
        except Exception as e:
            print(f"  Note: Query requires data for BTC-ETH, ETH-USDT, BTC-USDT")
            print(f"  Error: {e}")


# Main execution
if __name__ == "__main__":
    # Try to import config, fall back to defaults if not found
    try:
        from config import GCP_CONFIG, PATHS, LOAD_CONFIG
        print("✓ Loaded configuration from config.py")
    except ImportError:
        print("⚠ config.py not found, using default values")
        GCP_CONFIG = {
            'host': '34.73.134.13',
            'username': 'postgres',
            'password': 'dva_access',
            'database': 'postgres',
            'port': 5432
        }
        PATHS = {
            'parquet_directory': r'D:\DVA Project\Datasets\Binance Full History'
        }
        LOAD_CONFIG = {
            'table_name': 'ohlc_data',
            'chunk_size': 10000,
            'file_limit': None
        }
    
    # Initialize loader
    print("\nConnecting to GCP Cloud SQL PostgreSQL...")
    loader = BinanceGCPLoader(
        host=GCP_CONFIG['host'],
        username=GCP_CONFIG['username'],
        password=GCP_CONFIG['password'],
        database=GCP_CONFIG['database'],
        port=GCP_CONFIG.get('port', 5432)
    )
    
    # Create table with enhanced schema
    loader.create_ohlc_table(table_name=LOAD_CONFIG['table_name'])
    
    # Load all parquet files from directory
    loader.load_directory(
        directory=PATHS['parquet_directory'],
        table_name=LOAD_CONFIG['table_name'],
        chunk_size=LOAD_CONFIG['chunk_size'],
        limit=LOAD_CONFIG.get('file_limit')
    )
    
    # Get statistics
    loader.get_table_stats(table_name=LOAD_CONFIG['table_name'])
    
    # Show sample arbitrage query
    loader.sample_triangular_arbitrage_query(table_name=LOAD_CONFIG['table_name'])
    
    print("\n✓ All operations completed!")
