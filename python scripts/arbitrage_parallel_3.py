"""
Efficient Triangular Arbitrage Detection with Parallel Processing
For CSE6242 Cryptocurrency Market Efficiency Research
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import networkx as nx
from datetime import datetime, timedelta
import argparse
import json
from collections import defaultdict
import pickle
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count
import hashlib
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# ============================================================================
# PARAMETERS
# ============================================================================

class ArbitrageConfig:
    """Configuration for arbitrage detection"""
    
    def __init__(self):
        # Transaction costs
        self.fee_per_trade = 0.001  # 0.1% Binance standard fee
        self.num_trades = 3
        
        # Liquidity filters
        self.min_volume = 100
        self.min_quote_volume = 0
        self.min_trades = 5
        
        # Profit thresholds
        self.min_profit_pct = 0.1
        
        # Time filtering
        self.start_date = None
        self.end_date = None
        
        # Batch processing
        self.batch_size = 10000  # Rows per batch from DB
        self.timestamp_batch_size = 100  # Timestamps to process together
        
        # Parallel processing
        self.num_workers = cpu_count() - 1  # Leave one CPU free
        
        # Triangle cache
        self.triangle_cache_file = 'triangles_cache.pkl'
        
        # Run metadata
        self.run_id = None  # Will be generated
        self.run_time = None  # Will be set at runtime
        
    def fee_multiplier(self):
        return (1 - self.fee_per_trade) ** self.num_trades
    
    def to_dict(self):
        """Convert config to dictionary for storage"""
        return {
            'fee_per_trade': self.fee_per_trade,
            'fee_per_trade_pct': self.fee_per_trade * 100,
            'num_trades': self.num_trades,
            'total_fee_impact_pct': (1 - self.fee_multiplier()) * 100,
            'min_volume': self.min_volume,
            'min_quote_volume': self.min_quote_volume,
            'min_trades': self.min_trades,
            'min_profit_pct': self.min_profit_pct,
            'batch_size': self.batch_size,
            'timestamp_batch_size': self.timestamp_batch_size,
            'num_workers': self.num_workers,
            'start_date': str(self.start_date) if self.start_date else None,
            'end_date': str(self.end_date) if self.end_date else None,
        }
    
    def to_json(self):
        """Convert to JSON string for database storage"""
        return json.dumps(self.to_dict())
    
    def generate_run_id(self):
        """Generate unique run ID based on parameters and timestamp"""
        param_str = json.dumps(self.to_dict(), sort_keys=True)
        hash_obj = hashlib.md5(param_str.encode())
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.run_id = f"{timestamp}_{hash_obj.hexdigest()[:8]}"
        return self.run_id


# ============================================================================
# DATABASE CONNECTION HELPERS
# ============================================================================

def create_sqlalchemy_engine(host, database, user, password, port=5432):
    """Create SQLAlchemy engine for pandas operations"""
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    # Use NullPool for parallel processing to avoid connection pooling issues
    engine = create_engine(connection_string, poolclass=NullPool)
    return engine


def get_psycopg2_connection(host, database, user, password, port=5432):
    """Create psycopg2 connection for bulk inserts"""
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return conn


# ============================================================================
# DATABASE SCHEMA
# ============================================================================

def create_opportunities_table(engine):
    """
    Create the triangle_opportunities table if it doesn't exist
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS triangle_opportunities (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,
        run_time TIMESTAMP NOT NULL,
        run_parameters JSONB NOT NULL,
        
        -- Opportunity details
        timestamp TIMESTAMP NOT NULL,
        trade_date DATE NOT NULL,
        path VARCHAR(100) NOT NULL,
        triangle_key VARCHAR(100) NOT NULL,
        curr_a VARCHAR(20) NOT NULL,
        curr_b VARCHAR(20) NOT NULL,
        curr_c VARCHAR(20) NOT NULL,
        
        -- Profit metrics
        profit_raw_pct FLOAT NOT NULL,
        profit_net_pct FLOAT NOT NULL,
        
        -- Rates
        rate_ab FLOAT NOT NULL,
        rate_bc FLOAT NOT NULL,
        rate_ca FLOAT NOT NULL,
        
        -- Pair details
        pair_ab VARCHAR(50) NOT NULL,
        pair_bc VARCHAR(50) NOT NULL,
        pair_ca VARCHAR(50) NOT NULL,
        
        -- Direction details
        dir_ab VARCHAR(20) NOT NULL,
        dir_bc VARCHAR(20) NOT NULL,
        dir_ca VARCHAR(20) NOT NULL,
        
        -- Volume metrics
        min_volume FLOAT,
        volume_usd_ab FLOAT,
        volume_usd_bc FLOAT,
        volume_usd_ca FLOAT,
        min_quote_volume FLOAT,
        min_trades INTEGER,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes for efficient querying
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_run_id ON triangle_opportunities(run_id);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_timestamp ON triangle_opportunities(timestamp);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_trade_date ON triangle_opportunities(trade_date);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_profit ON triangle_opportunities(profit_net_pct DESC);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_currencies ON triangle_opportunities(curr_a, curr_b, curr_c);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_run_time ON triangle_opportunities(run_time);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_triangle_key ON triangle_opportunities(triangle_key);
    CREATE INDEX IF NOT EXISTS idx_triangle_opp_triangle_key_timestamp ON triangle_opportunities(triangle_key, timestamp);
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    print("Table 'triangle_opportunities' ready")


def insert_opportunities_batch(engine, opportunities_df, run_id, run_time, run_parameters):
    """
    Bulk insert opportunities into database using psycopg2 for better performance
    """
    if len(opportunities_df) == 0:
        return 0
    
    # Get raw connection from engine for bulk insert
    raw_conn = engine.raw_connection()
    
    try:
        insert_sql = """
        INSERT INTO triangle_opportunities (
            run_id, run_time, run_parameters,
            timestamp, trade_date, path, triangle_key, curr_a, curr_b, curr_c,
            profit_raw_pct, profit_net_pct,
            rate_ab, rate_bc, rate_ca,
            pair_ab, pair_bc, pair_ca,
            dir_ab, dir_bc, dir_ca,
            min_volume, volume_usd_ab, volume_usd_bc, volume_usd_ca,
            min_quote_volume, min_trades
        ) VALUES %s
        """
        
        # Prepare data for bulk insert
        values = []
        for _, row in opportunities_df.iterrows():
            values.append((
                run_id,
                run_time,
                run_parameters,
                row['timestamp'],
                row['trade_date'],
                row['path'],
                row['triangle_key'],
                row['curr_a'],
                row['curr_b'],
                row['curr_c'],
                row['profit_raw_pct'],
                row['profit_net_pct'],
                row['rate_ab'],
                row['rate_bc'],
                row['rate_ca'],
                row['pair_ab'],
                row['pair_bc'],
                row['pair_ca'],
                row['dir_ab'],
                row['dir_bc'],
                row['dir_ca'],
                row['min_volume'],
                row['volume_usd_ab'],
                row['volume_usd_bc'],
                row['volume_usd_ca'],
                row['min_quote_volume'],
                row['min_trades'],
            ))
        
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
            raw_conn.commit()
        
        return len(values)
        
    finally:
        raw_conn.close()


# ============================================================================
# GRAPH CONSTRUCTION
# ============================================================================

def get_all_trading_pairs(engine, table_name='ohlc_data'):
    """Get unique trading pairs from all_pairs table"""
    query = """
    SELECT DISTINCT 
        base_currency,
        quote_currency
    FROM all_pairs
    """
    df = pd.read_sql_query(query, engine)
    return df


def build_currency_graph(pairs_df):
    """Build undirected graph of currency relationships"""
    print("Building currency graph...")
    G = nx.Graph()
    
    for _, row in pairs_df.iterrows():
        base = row['base_currency']
        quote = row['quote_currency']
        G.add_edge(base, quote, pair=f"{base}-{quote}")
    
    print(f"  Nodes (currencies): {G.number_of_nodes()}")
    print(f"  Edges (trading pairs): {G.number_of_edges()}")
    
    return G


def find_all_triangles(G):
    """Find all triangles (3-cycles) in the graph"""
    print("\nFinding all triangles...")
    triangles = []
    
    for node in tqdm(G.nodes(), desc="Processing nodes"):
        neighbors = list(G.neighbors(node))
        
        for i in range(len(neighbors)):
            for j in range(i + 1, len(neighbors)):
                n1, n2 = neighbors[i], neighbors[j]
                
                if G.has_edge(n1, n2):
                    triangle = tuple(sorted([node, n1, n2]))
                    triangles.append(triangle)
    
    triangles = list(set(triangles))
    print(f"  Found {len(triangles)} unique triangles")
    
    return triangles


def save_triangles(triangles, filepath):
    """Save triangle data for reuse"""
    with open(filepath, 'wb') as f:
        pickle.dump(triangles, f)
    print(f"\nTriangles saved to {filepath}")


def load_triangles(filepath):
    """Load pre-computed triangles"""
    with open(filepath, 'rb') as f:
        triangles = pickle.load(f)
    print(f"Loaded {len(triangles)} triangles from {filepath}")
    return triangles


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_triangle_key(curr_a, curr_b, curr_c):
    """
    Generate triangle key: three currencies in alphabetical order separated by |
    
    Example: BTC, ETH, FUN -> BTC|ETH|FUN
    """
    currencies = sorted([curr_a, curr_b, curr_c])
    return '|'.join(currencies)


# ============================================================================
# PARALLEL PROCESSING - DATA FETCHING
# ============================================================================

def get_unique_timestamps(engine, table_name, config):
    """Get all unique timestamps from all_open_times table"""
    time_filter = "WHERE 1=1"
    if config.start_date:
        time_filter += f" AND open_time >= '{config.start_date}'"
    if config.end_date:
        time_filter += f" AND open_time < '{config.end_date}'"
    
    query = f"""
    SELECT open_time as timestamp
    FROM all_open_times
    {time_filter}
    ORDER BY open_time
    """
    
    df = pd.read_sql_query(query, engine)
    return df['timestamp'].tolist()


def fetch_price_data_for_timestamps(connection_string, table_name, timestamps, config):
    """
    Fetch price data for specific timestamps
    This function runs in parallel workers - each creates its own engine
    """
    # Each worker gets its own engine
    engine = create_engine(connection_string, poolclass=NullPool)
    
    try:
        # Convert timestamps to SQL-compatible format
        timestamp_list = ", ".join([f"'{ts}'" for ts in timestamps])
        
        query = f"""
        SELECT 
            open_time as timestamp,
            base_currency,
            quote_currency,
            low as forward_rate,
            high,
            1.0 / NULLIF(high, 0) as reverse_rate,
            volume,
            volume_usd,
            quote_asset_volume,
            number_of_trades
        FROM {table_name}
        WHERE open_time IN ({timestamp_list})
          AND volume_usd > {config.min_volume}
          AND close > 0
          AND low > 0
          AND high > 0
          AND number_of_trades >= {config.min_trades}
        ORDER BY open_time
        """
        
        df = pd.read_sql_query(query, engine)
        return df
        
    finally:
        engine.dispose()


# ============================================================================
# ARBITRAGE CALCULATION
# ============================================================================

def calculate_triangle_arbitrage(price_data, triangles, config):
    """
    Calculate arbitrage opportunities for pre-defined triangles
    Optimized for parallel processing
    """
    results = []
    
    # Group by timestamp
    grouped = price_data.groupby('timestamp')
    
    for timestamp, group in grouped:
        # Create lookup dict for this timestamp
        price_lookup = {}
        
        for _, row in group.iterrows():
            base = row['base_currency']
            quote = row['quote_currency']
            
            # Forward: base → quote
            price_lookup[(base, quote)] = {
                'rate': row['forward_rate'],
                'volume': row['volume'],
                'volume_usd': row['volume_usd'],
                'quote_volume': row['quote_asset_volume'],
                'trades': row['number_of_trades'],
                'pair': f"{base}-{quote}",
                'direction': 'forward'
            }
            
            # Reverse: quote → base
            price_lookup[(quote, base)] = {
                'rate': row['reverse_rate'],
                'volume': row['volume'],
                'volume_usd': row['volume_usd'],
                'quote_volume': row['quote_asset_volume'],
                'trades': row['number_of_trades'],
                'pair': f"{base}-{quote}",
                'direction': 'reverse'
            }
        
        # Check each triangle with all possible orderings
        for triangle in triangles:
            a, b, c = triangle
            
            paths = [
                (a, b, c, a),
                (a, c, b, a),
                (b, a, c, b),
                (b, c, a, b),
                (c, a, b, c),
                (c, b, a, c),
            ]
            
            for path in paths:
                curr_a, curr_b, curr_c, _ = path
                
                leg1 = price_lookup.get((curr_a, curr_b))
                leg2 = price_lookup.get((curr_b, curr_c))
                leg3 = price_lookup.get((curr_c, curr_a))
                
                if leg1 and leg2 and leg3:
                    mult_raw = leg1['rate'] * leg2['rate'] * leg3['rate']
                    mult_net = mult_raw * config.fee_multiplier()
                    
                    min_volume = min(leg1['volume_usd'], leg2['volume_usd'], leg3['volume_usd'])
                    min_quote_volume = min(leg1['quote_volume'], leg2['quote_volume'], leg3['quote_volume'])
                    
                    if min_quote_volume < config.min_quote_volume:
                        continue
                    
                    profit_net_pct = (mult_net - 1) * 100
                    
                    if profit_net_pct > config.min_profit_pct:
                        # Generate triangle key
                        triangle_key = generate_triangle_key(curr_a, curr_b, curr_c)
                        
                        # Extract trade date (date without time)
                        trade_date = pd.to_datetime(timestamp).date()
                        
                        results.append({
                            'timestamp': timestamp,
                            'trade_date': trade_date,
                            'path': f"{curr_a}→{curr_b}→{curr_c}→{curr_a}",
                            'triangle_key': triangle_key,
                            'curr_a': curr_a,
                            'curr_b': curr_b,
                            'curr_c': curr_c,
                            'profit_raw_pct': (mult_raw - 1) * 100,
                            'profit_net_pct': profit_net_pct,
                            'rate_ab': leg1['rate'],
                            'rate_bc': leg2['rate'],
                            'rate_ca': leg3['rate'],
                            'pair_ab': leg1['pair'],
                            'pair_bc': leg2['pair'],
                            'pair_ca': leg3['pair'],
                            'dir_ab': leg1['direction'],
                            'dir_bc': leg2['direction'],
                            'dir_ca': leg3['direction'],
                            'min_volume': min_volume,
                            'volume_usd_ab': leg1['volume_usd'],
                            'volume_usd_bc': leg2['volume_usd'],
                            'volume_usd_ca': leg3['volume_usd'],
                            'min_quote_volume': min_quote_volume,
                            'min_trades': min(leg1['trades'], leg2['trades'], leg3['trades']),
                        })
    
    return pd.DataFrame(results) if results else pd.DataFrame()


# ============================================================================
# PARALLEL WORKER FUNCTION
# ============================================================================

def process_timestamp_batch(args):
    """
    Worker function for parallel processing
    Each worker processes a batch of timestamps
    
    Returns: DataFrame with results
    """
    connection_string, table_name, timestamp_batch, triangles, config = args
    
    try:
        # Fetch data for this batch of timestamps
        price_data = fetch_price_data_for_timestamps(
            connection_string, table_name, timestamp_batch, config
        )
        
        if len(price_data) == 0:
            return pd.DataFrame()
        
        # Calculate arbitrage
        results = calculate_triangle_arbitrage(price_data, triangles, config)
        
        return results
        
    except Exception as e:
        print(f"Error processing batch: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# ============================================================================
# MAIN EXECUTION PIPELINE WITH PARALLELIZATION
# ============================================================================

def detect_arbitrage_parallel(engine, config, table_name='binance_1m_data', 
                              rebuild_triangles=False):
    """
    Main pipeline using parallel processing
    """
    print("=" * 80)
    print("TRIANGULAR ARBITRAGE DETECTION - PARALLEL GRAPH METHOD")
    print("=" * 80)
    print("\nConfiguration:")
    print(json.dumps(config.to_dict(), indent=2))
    print()
    
    # Generate run ID and set run time
    config.generate_run_id()
    config.run_time = datetime.now()
    run_parameters = config.to_json()
    
    print(f"Run ID: {config.run_id}")
    print(f"Run Time: {config.run_time}")
    print(f"Workers: {config.num_workers}")
    print()
    
    # Create table if needed
    create_opportunities_table(engine)
    
    # Step 1: Get or build triangles
    if rebuild_triangles or not os.path.exists(config.triangle_cache_file):
        print("\n" + "=" * 80)
        print("STEP 1: Building Triangle Structure")
        print("=" * 80)
        
        pairs_df = get_all_trading_pairs(engine, table_name)
        G = build_currency_graph(pairs_df)
        triangles = find_all_triangles(G)
        save_triangles(triangles, config.triangle_cache_file)
    else:
        print(f"\nLoading cached triangles from {config.triangle_cache_file}")
        triangles = load_triangles(config.triangle_cache_file)
    
    # Step 2: Get all unique timestamps
    print("\n" + "=" * 80)
    print("STEP 2: Getting Unique Timestamps")
    print("=" * 80)
    
    all_timestamps = get_unique_timestamps(engine, table_name, config)
    print(f"Found {len(all_timestamps)} unique timestamps to process")
    
    # Step 3: Divide timestamps into batches for parallel processing
    print("\n" + "=" * 80)
    print("STEP 3: Parallel Processing")
    print("=" * 80)
    
    timestamp_batches = [
        all_timestamps[i:i + config.timestamp_batch_size]
        for i in range(0, len(all_timestamps), config.timestamp_batch_size)
    ]
    
    print(f"Split into {len(timestamp_batches)} batches of ~{config.timestamp_batch_size} timestamps each")
    
    # Get connection string for workers
    #connection_string = str(engine.url)  ## this does not work. password is hidden
    
    # Get connection string for workers (MUST include password)
    connection_string = engine.url.render_as_string(hide_password=False)

    # Optional safety check so this never silently breaks again
    if ":***@" in connection_string:
        raise RuntimeError("Worker connection string has masked password")
    
    # Prepare arguments for parallel workers
    worker_args = [
        (connection_string, table_name, batch, triangles, config)
        for batch in timestamp_batches
    ]
    
    # Process in parallel
    total_inserted = 0
    
    print(f"\nProcessing {len(worker_args)} batches in parallel...")
    
    with Pool(processes=config.num_workers) as pool:
        # Process batches in parallel with progress bar
        for i, result_df in enumerate(tqdm(
            pool.imap(process_timestamp_batch, worker_args),
            total=len(worker_args),
            desc="Processing batches"
        )):
            # Insert results into database
            if len(result_df) > 0:
                inserted = insert_opportunities_batch(
                    engine, result_df, config.run_id, config.run_time, run_parameters
                )
                total_inserted += inserted
    
    print(f"\n{'=' * 80}")
    print(f"COMPLETE: Inserted {total_inserted} arbitrage opportunities")
    print(f"Run ID: {config.run_id}")
    print(f"{'=' * 80}")
    
    return config.run_id, total_inserted


# ============================================================================
# QUERY AND ANALYSIS FUNCTIONS
# ============================================================================

def get_run_statistics(engine, run_id):
    """Get statistics for a specific run"""
    query = f"""
    SELECT 
        COUNT(*) as total_opportunities,
        AVG(profit_net_pct) as avg_profit,
        STDDEV(profit_net_pct) as std_profit,
        MIN(profit_net_pct) as min_profit,
        MAX(profit_net_pct) as max_profit,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp,
        COUNT(DISTINCT timestamp) as unique_timestamps,
        COUNT(DISTINCT triangle_key) as unique_triangles,
        COUNT(DISTINCT trade_date) as unique_dates
    FROM triangle_opportunities
    WHERE run_id = '{run_id}'
    """
    
    df = pd.read_sql_query(query, engine)
    return df


def get_top_opportunities(engine, run_id, limit=10):
    """Get top opportunities from a run"""
    query = f"""
    SELECT *
    FROM triangle_opportunities
    WHERE run_id = '{run_id}'
    ORDER BY profit_net_pct DESC
    LIMIT {limit}
    """
    
    df = pd.read_sql_query(query, engine)
    return df


def compare_runs(engine, run_ids):
    """Compare statistics across multiple runs"""
    placeholders = ', '.join([f"'{rid}'" for rid in run_ids])
    
    query = f"""
    SELECT 
        run_id,
        run_time,
        run_parameters->>'fee_per_trade_pct' as fee_pct,
        run_parameters->>'min_profit_pct' as min_profit_pct,
        COUNT(*) as total_opportunities,
        AVG(profit_net_pct) as avg_profit,
        MAX(profit_net_pct) as max_profit,
        COUNT(DISTINCT triangle_key) as unique_triangles
    FROM triangle_opportunities
    WHERE run_id IN ({placeholders})
    GROUP BY run_id, run_time, run_parameters
    ORDER BY run_time DESC
    """
    
    df = pd.read_sql_query(query, engine)
    return df


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Detect triangular arbitrage with parallel processing and database storage'
    )
    
    # Database
    parser.add_argument('--host', default='34.73.134.13')
    parser.add_argument('--database', default='postgres')
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--password', default='dva_access')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--table', default='ohlc_data')
    
    # parser.add_argument('--host', required=True)
    # parser.add_argument('--database', required=True)
    # parser.add_argument('--user', required=True)
    # parser.add_argument('--password', required=True)
    # parser.add_argument('--port', type=int, default=5432)
    # parser.add_argument('--table', default='binance_1m_data')
    
    # Parameters
    parser.add_argument('--fee', type=float, default=0.001)
    parser.add_argument('--min-volume', type=float, default=100)
    parser.add_argument('--min-quote-volume', type=float, default=0)
    parser.add_argument('--min-trades', type=int, default=5)
    parser.add_argument('--min-profit', type=float, default=0.1)
    
    # Time filtering
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    # Processing
    parser.add_argument('--timestamp-batch-size', type=int, default=100,
                       help='Number of timestamps per parallel batch')
    parser.add_argument('--num-workers', type=int, default=None,
                       help='Number of parallel workers (default: CPU count - 1)')
    parser.add_argument('--rebuild-triangles', action='store_true')
    parser.add_argument('--triangle-cache', default='triangles_cache.pkl')
    
    # Analysis
    parser.add_argument('--analyze', help='Run ID to analyze')
    parser.add_argument('--compare', nargs='+', help='Run IDs to compare')
    
    args = parser.parse_args()
    
    # Create SQLAlchemy engine
    print("Creating database engine...")
    engine = create_sqlalchemy_engine(
        host=args.host,
        database=args.database,
        user=args.user,
        password=args.password,
        port=args.port
    )
    
    try:
        # Analysis mode
        if args.analyze:
            print(f"\nAnalyzing run: {args.analyze}")
            print("=" * 80)
            
            stats = get_run_statistics(engine, args.analyze)
            print("\nRun Statistics:")
            print(stats.to_string(index=False))
            
            top = get_top_opportunities(engine, args.analyze, 10)
            print("\nTop 10 Opportunities:")
            print(top[['timestamp', 'path', 'triangle_key', 'profit_net_pct']].to_string(index=False))
            
            return
        
        if args.compare:
            print(f"\nComparing {len(args.compare)} runs")
            print("=" * 80)
            
            comparison = compare_runs(engine, args.compare)
            print(comparison.to_string(index=False))
            
            return
        
        # Detection mode
        config = ArbitrageConfig()
        config.fee_per_trade = args.fee
        config.min_volume = args.min_volume
        config.min_quote_volume = args.min_quote_volume
        config.min_trades = args.min_trades
        config.min_profit_pct = args.min_profit
        config.start_date = args.start_date
        config.end_date = args.end_date
        config.timestamp_batch_size = args.timestamp_batch_size
        config.triangle_cache_file = args.triangle_cache
        
        if args.num_workers:
            config.num_workers = args.num_workers
        
        # Run detection
        start_time = datetime.now()
        run_id, total_inserted = detect_arbitrage_parallel(
            engine, config, args.table, args.rebuild_triangles
        )
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print(f"\nTotal processing time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        print(f"\nTo analyze this run, use:")
        print(f"  python {os.path.basename(__file__)} --analyze {run_id} --host {args.host} --database {args.database} --user {args.user} --password {args.password}")
        
    finally:
        engine.dispose()
        print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()

