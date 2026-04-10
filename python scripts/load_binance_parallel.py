#!/usr/bin/env python3
"""
PARALLEL Binance to GCP Loader - Uses all CPUs!
Loads multiple files simultaneously across available cores
"""
import pandas as pd
from sqlalchemy import create_engine, text, pool
from pathlib import Path
import os
from datetime import datetime
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

class ParallelBinanceLoader:
    def __init__(self, host, username, password, database='postgres', port=5432):
        """Initialize with connection details (not the engine - created per process)"""
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
    def get_engine(self):
        """Create engine for current process (thread-safe)"""
        return create_engine(
            self.db_url,
            poolclass=pool.NullPool,  # No connection pooling for parallel workers
            connect_args={'connect_timeout': 60}
        )
    
    def create_ohlc_table_optimized(self, table_name='ohlc_data'):
        """Create table WITHOUT indexes (add after loading)"""
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
            load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        engine.dispose()
        
        print(f"✓ Table '{table_name}' created (indexes will be added after loading)")
    
    def create_indexes(self, table_name='ohlc_data'):
        """Create indexes AFTER all data loaded"""
        print("\n" + "="*60)
        print("Creating Indexes (this may take 10-30 minutes)")
        print("="*60)
        
        start_time = time.time()
        # ALTER TABLE {table_name} ADD CONSTRAINT uq_{table_name}_pair_time 
            # UNIQUE (pair, open_time);        
        index_sql = f"""

        
        CREATE INDEX idx_{table_name}_open_time ON {table_name}(open_time);
        CREATE INDEX idx_{table_name}_pair ON {table_name}(pair);
        CREATE INDEX idx_{table_name}_base_curr ON {table_name}(base_currency);
        CREATE INDEX idx_{table_name}_quote_curr ON {table_name}(quote_currency);
        CREATE INDEX idx_{table_name}_pair_time ON {table_name}(pair, open_time);
        CREATE INDEX idx_{table_name}_currencies ON {table_name}(base_currency, quote_currency, open_time);
        
        VACUUM ANALYZE {table_name};
        """
        
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text(index_sql))
            conn.commit()
        engine.dispose()
        
        elapsed = time.time() - start_time
        print(f"✓ Indexes created in {elapsed/60:.1f} minutes")
    
    def get_loaded_pairs(self, table_name='ohlc_data'):
        """Get dictionary of loaded pairs and their row counts"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT pair, COUNT(*) as row_count
                    FROM {table_name}
                    GROUP BY pair
                """))
                loaded = {row[0]: row[1] for row in result}
            engine.dispose()
            return loaded
        except:
            return {}
    
    def get_parquet_row_count(self, parquet_file):
        """Get row count from parquet file"""
        df = pd.read_parquet(parquet_file, columns=[])
        return len(df)
    
    def parse_pair(self, filename):
        """Parse base and quote currencies from filename"""
        pair_name = Path(filename).stem
        if '-' in pair_name:
            parts = pair_name.split('-')
            base_currency = parts[0]
            quote_currency = parts[1]
        else:
            raise ValueError(f"Invalid filename: {filename}")
        return pair_name, base_currency, quote_currency
    
    def load_single_file(self, file_info):
        """
        Load a single file - designed to be called in parallel
        Returns: (success, pair_name, rows, elapsed, error_msg)
        """
        parquet_file, table_name, chunk_size = file_info
        
        try:
            start_time = time.time()
            pair_name, base_currency, quote_currency = self.parse_pair(parquet_file)
            
            # Read parquet (fast - 0.3s)
            df = pd.read_parquet(parquet_file)
            
            # Add metadata
            df['pair'] = pair_name
            df['base_currency'] = base_currency
            df['quote_currency'] = quote_currency
            df = df.reset_index()
            
            # Reorder columns
            columns = [
                'pair', 'base_currency', 'quote_currency', 'open_time',
                'open', 'high', 'low', 'close', 
                'volume', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
            ]
            df = df[columns]
            
            total_rows = len(df)
            
            # Create engine for this process
            engine = self.get_engine()
            
            # Insert in chunks
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(table_name, engine, if_exists='append', 
                           index=False, method='multi', chunksize=chunk_size)
            
            engine.dispose()
            
            elapsed = time.time() - start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            
            return (True, pair_name, total_rows, elapsed, rate, None)
            
        except Exception as e:
            return (False, Path(parquet_file).stem, 0, 0, 0, str(e))
    
    def load_directory_parallel(self, directory, table_name='ohlc_data', 
                               chunk_size=50000, max_workers=6, pattern='*.parquet'):
        """
        Load files in parallel using multiple CPU cores
        
        Args:
            directory: Directory with parquet files
            table_name: Target table
            chunk_size: Rows per insert batch
            max_workers: Number of parallel workers (recommended: num_cores - 2)
            pattern: File pattern
        """
        parquet_files = sorted(list(Path(directory).glob(pattern)))
        total_files = len(parquet_files)
        
        print(f"\n{'='*60}")
        print(f"PARALLEL LOADING with {max_workers} workers")
        print(f"Found {total_files} parquet files")
        print(f"{'='*60}")
        
        # Check for already loaded files
        print("\nChecking for previously loaded data...")
        loaded_pairs = self.get_loaded_pairs(table_name)
        
        if loaded_pairs:
            print(f"✓ Found {len(loaded_pairs)} already loaded pairs")
            print(f"  Total rows already in DB: {sum(loaded_pairs.values()):,}")
        
        # Determine which files to load
        files_to_load = []
        files_to_skip = []
        
        for file in parquet_files:
            pair_name = Path(file).stem
            parquet_rows = self.get_parquet_row_count(file)
            db_rows = loaded_pairs.get(pair_name, 0)
            
            if db_rows == 0:
                files_to_load.append(file)
            elif db_rows == parquet_rows:
                files_to_skip.append(file)
            else:
                # Partially loaded - delete and reload
                print(f"  ⚠ {pair_name}: incomplete ({db_rows}/{parquet_rows} rows)")
                engine = self.get_engine()
                with engine.connect() as conn:
                    conn.execute(text(f"DELETE FROM {table_name} WHERE pair = :pair"), 
                               {"pair": pair_name})
                    conn.commit()
                engine.dispose()
                files_to_load.append(file)
        
        if files_to_skip:
            print(f"\n✓ Skipping {len(files_to_skip)} already loaded files")
        
        if not files_to_load:
            print("\n✓ All files already loaded!")
            return
        
        print(f"\n{'='*60}")
        print(f"Loading {len(files_to_load)} files with {max_workers} parallel workers")
        print(f"{'='*60}\n")
        
        # Prepare work items
        work_items = [(f, table_name, chunk_size) for f in files_to_load]
        
        # Track progress
        completed = 0
        success_count = 0
        fail_count = 0
        total_rows = 0
        overall_start = time.time()
        
        # Process in parallel
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(self.load_single_file, item): item[0] 
                for item in work_items
            }
            
            # Process as they complete
            for future in as_completed(future_to_file):
                completed += 1
                file = future_to_file[future]
                
                try:
                    success, pair_name, rows, elapsed, rate, error = future.result()
                    
                    if success:
                        success_count += 1
                        total_rows += rows
                        
                        # Progress indicator
                        overall_elapsed = time.time() - overall_start
                        overall_rate = total_rows / overall_elapsed if overall_elapsed > 0 else 0
                        
                        print(f"[{completed:3d}/{len(files_to_load):3d}] ✓ {pair_name:20s} "
                              f"{rows:8,} rows in {elapsed:5.1f}s ({rate:8,.0f} r/s) | "
                              f"Overall: {overall_rate:8,.0f} r/s")
                    else:
                        fail_count += 1
                        print(f"[{completed:3d}/{len(files_to_load):3d}] ✗ {pair_name:20s} ERROR: {error}")
                        
                except Exception as e:
                    fail_count += 1
                    print(f"[{completed:3d}/{len(files_to_load):3d}] ✗ {Path(file).stem:20s} EXCEPTION: {e}")
        
        overall_elapsed = time.time() - overall_start
        
        print(f"\n{'='*60}")
        print(f"Parallel Load Complete!")
        print(f"{'='*60}")
        print(f"  ✓ Successful: {success_count}/{len(files_to_load)}")
        print(f"  ✗ Failed: {fail_count}/{len(files_to_load)}")
        print(f"  📊 Total rows loaded: {total_rows:,}")
        print(f"  ⏱ Total time: {overall_elapsed/60:.1f} minutes")
        print(f"  ⚡ Average rate: {total_rows/overall_elapsed:,.0f} rows/sec")
        print(f"  🚀 Throughput: {len(files_to_load)/overall_elapsed*60:.1f} files/hour")
        print(f"{'='*60}")
    
    def get_table_stats(self, table_name='ohlc_data'):
        """Get statistics about loaded data"""
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT pair) as unique_pairs,
            MIN(open_time) as earliest_date,
            MAX(open_time) as latest_date,
            pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size
        FROM {table_name};
        """
        
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(stats_sql))
            stats = result.fetchone()
        engine.dispose()
        
        print(f"\n{'='*60}")
        print(f"Database Statistics - {table_name}")
        print(f"{'='*60}")
        print(f"  Total rows: {stats[0]:,}")
        print(f"  Unique pairs: {stats[1]:,}")
        print(f"  Date range: {stats[2]} to {stats[3]}")
        print(f"  Total size: {stats[4]}")
        print(f"{'='*60}")


def main():
    """Main execution with parallel loading"""
    
    # Configuration
    GCP_CONFIG = {
        'host': '34.73.134.13',
        'username': 'postgres',
        'password': 'dva_access',
        'database': 'postgres',
        'port': 5432
    }
    
    PARQUET_DIR = '/dva/binance-full-history'
    TABLE_NAME = 'ohlc_data'
    
    # Calculate optimal worker count
    cpu_count = mp.cpu_count()
    max_workers = max(1, cpu_count - 2)  # Leave 2 CPUs for system
    
    print(f"System has {cpu_count} CPUs")
    print(f"Using {max_workers} parallel workers")
    
    # Initialize loader
    loader = ParallelBinanceLoader(
        host=GCP_CONFIG['host'],
        username=GCP_CONFIG['username'],
        password=GCP_CONFIG['password'],
        database=GCP_CONFIG['database'],
        port=GCP_CONFIG['port']
    )
    
    # Ask user what to do
    print("\nOptions:")
    print("1. Start fresh (drop table and reload everything)")
    print("2. Resume from where left off (recommended)")
    choice = input("\nChoose (1 or 2): ").strip()
    
    if choice == '1':
        # Fresh start
        loader.create_ohlc_table_optimized(TABLE_NAME)
        
        # Load all files in parallel
        loader.load_directory_parallel(
            directory=PARQUET_DIR,
            table_name=TABLE_NAME,
            chunk_size=100000,
            max_workers=max_workers
        )
        
        # Create indexes AFTER loading
        loader.create_indexes(TABLE_NAME)
    else:
        # Resume
        loader.load_directory_parallel(
            directory=PARQUET_DIR,
            table_name=TABLE_NAME,
            chunk_size=100000,
            max_workers=max_workers
        )
    
    # Show final stats
    loader.get_table_stats(TABLE_NAME)
    
    print("\n✓ All operations completed!")


if __name__ == "__main__":
    main()
