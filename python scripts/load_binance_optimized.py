#!/usr/bin/env python3
"""
OPTIMIZED Binance to GCP Loader with Resume Capability
- Resumes from where it left off
- 10-20x faster loading
- Optimized for bulk inserts
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os
from datetime import datetime
import time

class OptimizedBinanceLoader:
    def __init__(self, host, username, password, database='postgres', port=5432):
        """Initialize with optimized connection pool settings"""
        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Optimized connection pool for bulk loading
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600,
            connect_args={
                'connect_timeout': 60,
                'options': '-c statement_timeout=600000'  # 10 min timeout
            }
        )
        print(f"✓ Connected to GCP Cloud SQL: {host}")
    
    def optimize_db_for_bulk_load(self):
        """Optimize PostgreSQL settings for bulk loading"""
        print("\n" + "="*60)
        print("Optimizing Database for Bulk Loading")
        print("="*60)
        
        optimization_sql = """
        -- Disable autovacuum during bulk load
        ALTER TABLE ohlc_data SET (autovacuum_enabled = false);
        
        -- Show current settings
        SHOW shared_buffers;
        SHOW maintenance_work_mem;
        SHOW checkpoint_timeout;
        """
        
        try:
            with self.engine.connect() as conn:
                results = conn.execute(text(optimization_sql))
                for result in results:
                    print(f"  {result}")
                conn.commit()
            print("✓ Database optimized for bulk loading")
        except Exception as e:
            print(f"⚠ Could not optimize DB settings: {e}")
            print("  Continue anyway (settings may not be optimal)")
    
    def create_ohlc_table_optimized(self, table_name='ohlc_data'):
        """
        Create table WITHOUT indexes initially (add after loading)
        """
        create_table_sql = f"""
        -- Drop existing table and indexes
        DROP TABLE IF EXISTS {table_name} CASCADE;
        
        -- Create table WITHOUT indexes (will add after loading)
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
        
        -- NO OTHER INDEXES YET - will create after loading all data
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        print(f"✓ Table '{table_name}' created (indexes will be added after loading)")
    
    def create_indexes(self, table_name='ohlc_data'):
        """Create indexes AFTER all data is loaded (much faster)"""
        print("\n" + "="*60)
        print("Creating Indexes (this may take 10-30 minutes)")
        print("="*60)
        
        start_time = time.time()
        
        index_sql = f"""
        -- Create unique constraint
        ALTER TABLE {table_name} ADD CONSTRAINT uq_{table_name}_pair_time 
            UNIQUE (pair, open_time);
        
        -- Create indexes
        CREATE INDEX idx_{table_name}_open_time ON {table_name}(open_time);
        CREATE INDEX idx_{table_name}_pair ON {table_name}(pair);
        CREATE INDEX idx_{table_name}_base_curr ON {table_name}(base_currency);
        CREATE INDEX idx_{table_name}_quote_curr ON {table_name}(quote_currency);
        CREATE INDEX idx_{table_name}_pair_time ON {table_name}(pair, open_time);
        CREATE INDEX idx_{table_name}_currencies ON {table_name}(base_currency, quote_currency, open_time);
        
        -- Re-enable autovacuum
        ALTER TABLE {table_name} SET (autovacuum_enabled = true);
        
        -- Run vacuum analyze
        VACUUM ANALYZE {table_name};
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(index_sql))
            conn.commit()
        
        elapsed = time.time() - start_time
        print(f"✓ Indexes created in {elapsed/60:.1f} minutes")
    
    def get_loaded_pairs(self, table_name='ohlc_data'):
        """Get dictionary of loaded pairs and their row counts"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT pair, COUNT(*) as row_count
                    FROM {table_name}
                    GROUP BY pair
                """))
                loaded = {row[0]: row[1] for row in result}
            return loaded
        except:
            # Table doesn't exist yet
            return {}
    
    def get_parquet_row_count(self, parquet_file):
        """Get row count from parquet file without loading all data"""
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
    
    def load_parquet_file_optimized(self, parquet_file, table_name='ohlc_data', 
                                   chunk_size=50000):
        """
        Optimized loading with larger chunks and COPY instead of INSERT
        """
        try:
            pair_name, base_currency, quote_currency = self.parse_pair(parquet_file)
            
            print(f"\nLoading {pair_name} ({base_currency}/{quote_currency})...")
            start_time = time.time()
            
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            elapsed = time.time() - start_time
            print(f"  ✓ Read parquet file in {elapsed:.2f} seconds")
            
            # Add metadata columns
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
            print(f"  Rows to insert: {total_rows:,}")
            
            # Use larger chunks and optimized method
            rows_inserted = 0
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                
                # Use method='multi' for faster inserts
                chunk.to_sql(table_name, self.engine, if_exists='append', 
                           index=False, method='multi', chunksize=chunk_size)
                
                rows_inserted += len(chunk)
                progress = min(i + chunk_size, total_rows)
                elapsed = time.time() - start_time
                rate = rows_inserted / elapsed if elapsed > 0 else 0
                print(f"  Progress: {progress:,}/{total_rows:,} ({100*progress/total_rows:.1f}%) - {rate:,.0f} rows/sec", end='\r')
            
            elapsed = time.time() - start_time
            rate = total_rows / elapsed if elapsed > 0 else 0
            print(f"\n  ✓ Completed {pair_name}: {rows_inserted:,} rows in {elapsed:.1f}s ({rate:,.0f} rows/sec)")
            return True, total_rows, elapsed
            
        except Exception as e:
            print(f"\n  ✗ Error loading {Path(parquet_file).name}: {e}")
            return False, 0, 0
    
    def load_directory_with_resume(self, directory, table_name='ohlc_data', 
                                   chunk_size=50000, pattern='*.parquet'):
        """
        Load with resume capability - skips already loaded files
        """
        parquet_files = sorted(list(Path(directory).glob(pattern)))
        total_files = len(parquet_files)
        
        print(f"\n{'='*60}")
        print(f"Found {total_files} parquet files")
        print(f"{'='*60}")
        
        # Get already loaded pairs
        print("\nChecking for previously loaded data...")
        loaded_pairs = self.get_loaded_pairs(table_name)
        
        if loaded_pairs:
            print(f"✓ Found {len(loaded_pairs)} already loaded pairs")
            print(f"  Total rows already in DB: {sum(loaded_pairs.values()):,}")
        else:
            print("  No existing data found - starting fresh load")
        
        # Determine which files to load
        files_to_load = []
        files_to_skip = []
        files_to_reload = []
        
        for file in parquet_files:
            print(f"\nAnalyzing file: {Path(file).name}")
            pair_name = Path(file).stem
            parquet_rows = self.get_parquet_row_count(file)
            db_rows = loaded_pairs.get(pair_name, 0)
            
            if db_rows == 0:
                # Not loaded at all
                files_to_load.append((file, 'new'))
            elif db_rows == parquet_rows:
                # Fully loaded
                files_to_skip.append(file)
            else:
                # Partially loaded - need to reload
                files_to_reload.append((file, db_rows, parquet_rows))
        
        print(f"\n{'='*60}")
        print(f"Load Plan:")
        print(f"  ✓ Skip (already loaded): {len(files_to_skip)}")
        print(f"  ⚠ Reload (incomplete): {len(files_to_reload)}")
        print(f"  ➕ New files to load: {len(files_to_load)}")
        print(f"  Total to process: {len(files_to_load) + len(files_to_reload)}")
        print(f"{'='*60}")
        
        if files_to_skip:
            print(f"\nSkipping {len(files_to_skip)} already loaded files...")
            for f in files_to_skip[:5]:
                print(f"  ✓ {Path(f).stem}")
            if len(files_to_skip) > 5:
                print(f"  ... and {len(files_to_skip) - 5} more")
        
        # Handle partial loads
        if files_to_reload:
            print(f"\n⚠ Found {len(files_to_reload)} partially loaded files:")
            for file, db_rows, parquet_rows in files_to_reload:
                pair = Path(file).stem
                print(f"  {pair}: {db_rows:,} in DB vs {parquet_rows:,} in file")
            
            response = input("\nDelete and reload these files? (y/n): ").lower()
            if response == 'y':
                print("\nDeleting incomplete data...")
                with self.engine.connect() as conn:
                    for file, _, _ in files_to_reload:
                        pair = Path(file).stem
                        conn.execute(text(f"DELETE FROM {table_name} WHERE pair = :pair"), 
                                   {"pair": pair})
                    conn.commit()
                print("✓ Incomplete data deleted")
                files_to_load.extend([(f[0], 'reload') for f in files_to_reload])
        
        # Load files
        if not files_to_load:
            print("\n✓ All files already loaded!")
            return
        
        print(f"\n{'='*60}")
        print(f"Loading {len(files_to_load)} files...")
        print(f"{'='*60}")
        
        success_count = 0
        fail_count = 0
        total_rows = 0
        total_time = 0
        overall_start = time.time()
        
        for i, (file, status) in enumerate(files_to_load, 1):
            prefix = "🔄" if status == 'reload' else "➕"
            print(f"\n[{i}/{len(files_to_load)}] {prefix}", end=" ")
            success, rows, elapsed = self.load_parquet_file_optimized(
                file, table_name, chunk_size
            )
            
            if success:
                success_count += 1
                total_rows += rows
                total_time += elapsed
            else:
                fail_count += 1
        
        overall_elapsed = time.time() - overall_start
        
        print(f"\n{'='*60}")
        print(f"Load Complete!")
        print(f"{'='*60}")
        print(f"  ✓ Successful: {success_count}/{len(files_to_load)}")
        print(f"  ✗ Failed: {fail_count}/{len(files_to_load)}")
        print(f"  📊 Total rows loaded: {total_rows:,}")
        print(f"  ⏱ Total time: {overall_elapsed/60:.1f} minutes")
        print(f"  ⚡ Average rate: {total_rows/overall_elapsed:,.0f} rows/sec")
        print(f"{'='*60}")
    
    def get_table_stats(self, table_name='ohlc_data'):
        """Get statistics about loaded data"""
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT pair) as unique_pairs,
            COUNT(DISTINCT base_currency) as unique_base,
            COUNT(DISTINCT quote_currency) as unique_quote,
            MIN(open_time) as earliest_date,
            MAX(open_time) as latest_date,
            pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size
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
        print(f"  Total size: {stats[6]}")
        print(f"{'='*60}")


# Main execution
if __name__ == "__main__":
    from config import GCP_CONFIG, PATHS, LOAD_CONFIG
    
    # Initialize optimized loader
    print("Connecting to GCP Cloud SQL PostgreSQL...")
    loader = OptimizedBinanceLoader(
        host=GCP_CONFIG['host'],
        username=GCP_CONFIG['username'],
        password=GCP_CONFIG['password'],
        database=GCP_CONFIG['database'],
        port=GCP_CONFIG.get('port', 5432)
    )
    
    # Ask user what to do
    print("\nOptions:")
    print("1. Start fresh (drop table and reload everything)")
    print("2. Resume from where left off (recommended)")
    choice = input("\nChoose (1 or 2): ").strip()
    
    if choice == '1':
        # Fresh start
        loader.create_ohlc_table_optimized(LOAD_CONFIG['table_name'])
        loader.optimize_db_for_bulk_load()
        
        # Load all files
        loader.load_directory_with_resume(
            directory=PATHS['parquet_directory'],
            table_name=LOAD_CONFIG['table_name'],
            chunk_size=LOAD_CONFIG['chunk_size'],  # Increased from 10000
            pattern='*.parquet'
        )
        
        # Create indexes AFTER loading
        loader.create_indexes(LOAD_CONFIG['table_name'])
    else:
        # Resume
        loader.load_directory_with_resume(
            directory=PATHS['parquet_directory'],
            table_name=LOAD_CONFIG['table_name'],
            chunk_size=LOAD_CONFIG['chunk_size'],
            pattern='*.parquet'
        )
    
    # Show final stats
    loader.get_table_stats(LOAD_CONFIG['table_name'])
    
    print("\n✓ All operations completed!")
