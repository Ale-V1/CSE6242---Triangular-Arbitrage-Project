#!/usr/bin/env python3
"""
Verify Data Integrity - Compare Parquet Files vs Database
Checks row counts to identify incomplete or failed loads
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime
import sys

class DataIntegrityChecker:
    def __init__(self, host, username, password, database='postgres', port=5432):
        """Initialize database connection"""
        db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(db_url, pool_pre_ping=True)
        print(f"✓ Connected to GCP Cloud SQL: {host}")
    
    def get_parquet_counts(self, directory, pattern='*.parquet'):
        """Get row counts from all parquet files"""
        print(f"\nScanning parquet files in {directory}...")
        
        parquet_files = sorted(list(Path(directory).glob(pattern)))
        parquet_counts = {}
        
        for i, file in enumerate(parquet_files, 1):
            pair_name = Path(file).stem
            try:
                # Fast count without loading data
                df = pd.read_parquet(file, columns=[])
                row_count = len(df)
                parquet_counts[pair_name] = row_count
                
                if i % 100 == 0:
                    print(f"  Scanned {i}/{len(parquet_files)} files...")
            except Exception as e:
                print(f"  ✗ Error reading {file.name}: {e}")
                parquet_counts[pair_name] = -1  # Mark as error
        
        print(f"✓ Scanned {len(parquet_files)} parquet files")
        return parquet_counts
    
    def get_database_counts(self, table_name='ohlc_data'):
        """Get row counts from database for each pair"""
        print(f"\nQuerying database for pair counts...")
        
        query = f"""
        SELECT pair, COUNT(*) as row_count
        FROM {table_name}
        GROUP BY pair
        ORDER BY pair;
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            db_counts = {row[0]: row[1] for row in result}
        
        print(f"✓ Found {len(db_counts)} pairs in database")
        return db_counts
    
    def compare_counts(self, parquet_counts, db_counts):
        """Compare parquet vs database counts"""
        print(f"\n{'='*80}")
        print("DATA INTEGRITY VERIFICATION")
        print(f"{'='*80}")
        
        all_pairs = sorted(set(parquet_counts.keys()) | set(db_counts.keys()))
        
        matches = []
        mismatches = []
        missing_in_db = []
        extra_in_db = []
        parquet_errors = []
        
        for pair in all_pairs:
            parquet_count = parquet_counts.get(pair, 0)
            db_count = db_counts.get(pair, 0)
            
            # Check for parquet read errors
            if parquet_count == -1:
                parquet_errors.append(pair)
                continue
            
            # Check for exact matches
            if parquet_count == db_count and parquet_count > 0:
                matches.append((pair, parquet_count))
            
            # Check for mismatches
            elif parquet_count > 0 and db_count > 0 and parquet_count != db_count:
                diff = db_count - parquet_count
                diff_pct = (diff / parquet_count) * 100
                mismatches.append((pair, parquet_count, db_count, diff, diff_pct))
            
            # Check for missing in DB
            elif parquet_count > 0 and db_count == 0:
                missing_in_db.append((pair, parquet_count))
            
            # Check for extra in DB (shouldn't happen)
            elif parquet_count == 0 and db_count > 0:
                extra_in_db.append((pair, db_count))
        
        return {
            'matches': matches,
            'mismatches': mismatches,
            'missing_in_db': missing_in_db,
            'extra_in_db': extra_in_db,
            'parquet_errors': parquet_errors,
            'total_pairs': len(all_pairs)
        }
    
    def print_report(self, results):
        """Print detailed verification report"""
        
        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Total pairs analyzed: {results['total_pairs']}")
        print(f"  ✓ Perfect matches: {len(results['matches'])}")
        print(f"  ⚠ Mismatches (incomplete): {len(results['mismatches'])}")
        print(f"  ✗ Missing in DB: {len(results['missing_in_db'])}")
        print(f"  ? Extra in DB: {len(results['extra_in_db'])}")
        print(f"  ! Parquet read errors: {len(results['parquet_errors'])}")
        
        # Calculate success rate
        success_rate = (len(results['matches']) / results['total_pairs']) * 100
        print(f"\nSuccess rate: {success_rate:.2f}%")
        
        # Mismatches detail
        if results['mismatches']:
            print(f"\n{'='*80}")
            print(f"MISMATCHES - Incomplete Loads ({len(results['mismatches'])} pairs)")
            print(f"{'='*80}")
            print(f"{'Pair':<25} {'Expected':<12} {'Actual':<12} {'Diff':<12} {'Diff %':<10}")
            print(f"{'-'*80}")
            
            for pair, expected, actual, diff, diff_pct in sorted(results['mismatches']):
                print(f"{pair:<25} {expected:>11,} {actual:>11,} {diff:>11,} {diff_pct:>9.2f}%")
            
            # Total missing rows
            total_missing = sum(exp - act for _, exp, act, _, _ in results['mismatches'])
            total_expected = sum(exp for _, exp, _, _, _ in results['mismatches'])
            print(f"{'-'*80}")
            print(f"{'TOTAL':<25} {total_expected:>11,} {'-':>11} {-total_missing:>11,}")
        
        # Missing in DB
        if results['missing_in_db']:
            print(f"\n{'='*80}")
            print(f"MISSING IN DATABASE ({len(results['missing_in_db'])} pairs)")
            print(f"{'='*80}")
            print(f"{'Pair':<25} {'Rows in Parquet':<15}")
            print(f"{'-'*80}")
            
            for pair, count in sorted(results['missing_in_db']):
                print(f"{pair:<25} {count:>14,}")
            
            total_missing_rows = sum(count for _, count in results['missing_in_db'])
            print(f"{'-'*80}")
            print(f"{'TOTAL':<25} {total_missing_rows:>14,}")
        
        # Extra in DB (unexpected)
        if results['extra_in_db']:
            print(f"\n{'='*80}")
            print(f"EXTRA IN DATABASE - Not in Parquet Files ({len(results['extra_in_db'])} pairs)")
            print(f"{'='*80}")
            for pair, count in sorted(results['extra_in_db']):
                print(f"  {pair}: {count:,} rows")
        
        # Parquet errors
        if results['parquet_errors']:
            print(f"\n{'='*80}")
            print(f"PARQUET READ ERRORS ({len(results['parquet_errors'])} files)")
            print(f"{'='*80}")
            for pair in sorted(results['parquet_errors']):
                print(f"  {pair}")
        
        # Perfect matches (optional - just summary)
        if results['matches']:
            print(f"\n{'='*80}")
            print(f"✓ PERFECT MATCHES ({len(results['matches'])} pairs)")
            print(f"{'='*80}")
            total_rows = sum(count for _, count in results['matches'])
            print(f"Total rows verified: {total_rows:,}")
            
            # Show first 10 as examples
            print(f"\nFirst 10 matches:")
            for pair, count in sorted(results['matches'])[:10]:
                print(f"  {pair:<25} {count:>11,} rows")
            if len(results['matches']) > 10:
                print(f"  ... and {len(results['matches']) - 10} more")
    
    def save_report(self, results, output_file='data_integrity_report.txt'):
        """Save report to file"""
        with open(output_file, 'w') as f:
            # Redirect print to file
            original_stdout = sys.stdout
            sys.stdout = f
            
            print(f"Data Integrity Report")
            print(f"Generated: {datetime.now()}")
            print(f"{'='*80}\n")
            
            self.print_report(results)
            
            sys.stdout = original_stdout
        
        print(f"\n✓ Report saved to: {output_file}")
    
    def get_reload_list(self, results):
        """Get list of pairs that need to be reloaded"""
        reload_pairs = []
        
        # Add mismatches
        for pair, expected, actual, diff, diff_pct in results['mismatches']:
            reload_pairs.append((pair, 'incomplete', f"{actual}/{expected} rows"))
        
        # Add missing
        for pair, count in results['missing_in_db']:
            reload_pairs.append((pair, 'missing', f"0/{count} rows"))
        
        # Add parquet errors
        for pair in results['parquet_errors']:
            reload_pairs.append((pair, 'parquet_error', 'Cannot read file'))
        
        return reload_pairs


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
    
    print("="*80)
    print("DATA INTEGRITY VERIFICATION TOOL")
    print("="*80)
    
    # Initialize checker
    checker = DataIntegrityChecker(
        host=GCP_CONFIG['host'],
        username=GCP_CONFIG['username'],
        password=GCP_CONFIG['password'],
        database=GCP_CONFIG['database'],
        port=GCP_CONFIG['port']
    )
    
    # Get counts from parquet files
    parquet_counts = checker.get_parquet_counts(PARQUET_DIR)
    
    # Get counts from database
    db_counts = checker.get_database_counts(TABLE_NAME)
    
    # Compare
    results = checker.compare_counts(parquet_counts, db_counts)
    
    # Print report
    checker.print_report(results)
    
    # Save report
    checker.save_report(results, 'data_integrity_report.txt')
    
    # Generate reload list if needed
    reload_list = checker.get_reload_list(results)
    
    if reload_list:
        print(f"\n{'='*80}")
        print(f"PAIRS NEEDING ATTENTION ({len(reload_list)} total)")
        print(f"{'='*80}")
        
        # Save to file
        with open('pairs_to_reload.txt', 'w') as f:
            f.write("# Pairs that need to be reloaded\n")
            f.write(f"# Generated: {datetime.now()}\n\n")
            
            for pair, reason, detail in reload_list:
                f.write(f"{pair}\t{reason}\t{detail}\n")
        
        print(f"✓ List saved to: pairs_to_reload.txt")
        
        # Summary by reason
        from collections import Counter
        reason_counts = Counter(reason for _, reason, _ in reload_list)
        print(f"\nBreakdown:")
        for reason, count in reason_counts.items():
            print(f"  {reason}: {count}")
    else:
        print(f"\n{'='*80}")
        print("✓ ALL DATA VERIFIED - NO ISSUES FOUND!")
        print(f"{'='*80}")
    
    print(f"\n{'='*80}")
    print("VERIFICATION COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
