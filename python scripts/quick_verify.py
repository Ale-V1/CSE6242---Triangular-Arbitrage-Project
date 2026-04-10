#!/usr/bin/env python3
"""
Quick Data Verification - Just show the problems
"""
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

def quick_verify(parquet_dir, db_host, db_user, db_pass, db_name='postgres'):
    """Quick check - only show mismatches"""
    
    print("Connecting to database...")
    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}")
    
    print("Getting database counts...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT pair, COUNT(*) FROM ohlc_data GROUP BY pair"))
        db_counts = {row[0]: row[1] for row in result}
    
    print(f"Checking {len(list(Path(parquet_dir).glob('*.parquet')))} parquet files...\n")
    
    problems = []
    checked = 0
    
    for file in sorted(Path(parquet_dir).glob('*.parquet')):
        pair = file.stem
        parquet_count = len(pd.read_parquet(file, columns=[]))
        db_count = db_counts.get(pair, 0)
        
        if parquet_count != db_count:
            problems.append((pair, parquet_count, db_count))
        
        checked += 1
        if checked % 100 == 0:
            print(f"Checked {checked} files...", end='\r')
    
    print(f"\n{'='*70}")
    print(f"VERIFICATION RESULTS")
    print(f"{'='*70}")
    print(f"Total pairs checked: {checked}")
    print(f"Perfect matches: {checked - len(problems)}")
    print(f"Issues found: {len(problems)}")
    
    if problems:
        print(f"\n{'='*70}")
        print(f"INCOMPLETE LOADS:")
        print(f"{'='*70}")
        print(f"{'Pair':<25} {'Expected':<12} {'In DB':<12} {'Missing':<12}")
        print(f"{'-'*70}")
        
        for pair, expected, actual in sorted(problems):
            missing = expected - actual
            print(f"{pair:<25} {expected:>11,} {actual:>11,} {missing:>11,}")
        
        print(f"\nTo reload these pairs, use:")
        print(f"  python reload_failed_pairs.py")
    else:
        print(f"\n✓ ALL DATA VERIFIED - NO ISSUES!")
    
    engine.dispose()


if __name__ == "__main__":
    quick_verify(
        parquet_dir='/dva/binance-full-history',
        db_host='34.73.134.13',
        db_user='postgres',
        db_pass='dva_access'
    )
