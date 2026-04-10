#!/usr/bin/env python3
"""
Reload Failed/Incomplete Pairs
Reads pairs_to_reload.txt and reloads only those pairs
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime
import time

class FailedPairReloader:
    def __init__(self, host, username, password, database='postgres', port=5432):
        """Initialize database connection"""
        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(db_url, pool_pre_ping=True)
    
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
    
    def reload_pair(self, parquet_file, table_name='ohlc_data', chunk_size=50000):
        """Reload a single pair"""
        try:
            start_time = time.time()
            pair_name, base_currency, quote_currency = self.parse_pair(parquet_file)
            
            # Delete existing data for this pair
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"DELETE FROM {table_name} WHERE pair = :pair"),
                    {"pair": pair_name}
                )
                deleted_rows = result.rowcount
                conn.commit()
            
            print(f"  Deleted {deleted_rows:,} existing rows")
            
            # Read parquet
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
            
            # Insert in chunks
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                chunk.to_sql(table_name, self.engine, if_exists='append', 
                           index=False, method='multi', chunksize=chunk_size)
            
            elapsed = time.time() - start_time
            print(f"  ✓ Reloaded {total_rows:,} rows in {elapsed:.1f}s")
            return True, total_rows
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return False, 0
    
    def reload_from_list(self, pairs_file, parquet_dir, table_name='ohlc_data'):
        """Reload all pairs from a list file"""
        
        # Read pairs to reload
        pairs_to_reload = []
        
        with open(pairs_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split('\t')
                    pair = parts[0]
                    pairs_to_reload.append(pair)
        
        print(f"{'='*70}")
        print(f"RELOADING FAILED PAIRS")
        print(f"{'='*70}")
        print(f"Pairs to reload: {len(pairs_to_reload)}\n")
        
        success_count = 0
        fail_count = 0
        total_rows = 0
        
        for i, pair in enumerate(pairs_to_reload, 1):
            parquet_file = Path(parquet_dir) / f"{pair}.parquet"
            
            if not parquet_file.exists():
                print(f"[{i}/{len(pairs_to_reload)}] ✗ {pair}: Parquet file not found")
                fail_count += 1
                continue
            
            print(f"[{i}/{len(pairs_to_reload)}] Reloading {pair}...")
            success, rows = self.reload_pair(parquet_file, table_name)
            
            if success:
                success_count += 1
                total_rows += rows
            else:
                fail_count += 1
        
        print(f"\n{'='*70}")
        print(f"RELOAD COMPLETE")
        print(f"{'='*70}")
        print(f"  ✓ Success: {success_count}/{len(pairs_to_reload)}")
        print(f"  ✗ Failed: {fail_count}/{len(pairs_to_reload)}")
        print(f"  📊 Total rows reloaded: {total_rows:,}")
        print(f"{'='*70}")


def main():
    """Main execution"""
    
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
    PAIRS_FILE = 'pairs_to_reload.txt'
    
    # Check if pairs file exists
    if not Path(PAIRS_FILE).exists():
        print(f"Error: {PAIRS_FILE} not found")
        print(f"Run verify_data_integrity.py first to generate this file")
        return
    
    # Initialize reloader
    reloader = FailedPairReloader(
        host=GCP_CONFIG['host'],
        username=GCP_CONFIG['username'],
        password=GCP_CONFIG['password'],
        database=GCP_CONFIG['database'],
        port=GCP_CONFIG['port']
    )
    
    # Reload failed pairs
    reloader.reload_from_list(PAIRS_FILE, PARQUET_DIR, TABLE_NAME)


if __name__ == "__main__":
    main()
