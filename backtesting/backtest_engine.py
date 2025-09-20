import os
import datetime  # type: ignore
import pandas as pd  # type: ignore
import logging

from core.brokerage_client import BrokerageClient
from config.config import INTRADAY_TARGET_STOCK
from strategies.macd_strategy import MACDStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STOCK_DATA_FOLDER = "stock_data"
REPORT_DUMP_FOLDER = "backtesting/report_dump"
DAYS = 60

# Timeframe configurations
TIMEFRAMES = {
    "1m": {"resample_factor": 1, "description": "1 Minute"},
    "10m": {"resample_factor": 10, "description": "10 Minutes"},
    "30m": {"resample_factor": 30, "description": "30 Minutes"},
    "1h": {"resample_factor": 60, "description": "1 Hour"},
    "3h": {"resample_factor": 180, "description": "3 Hours"},
    "1d": {"resample_factor": None, "description": "1 Day"}
}


def ensure_data_folder():
    os.makedirs(STOCK_DATA_FOLDER, exist_ok=True)


def ensure_report_dump_folder():
    os.makedirs(REPORT_DUMP_FOLDER, exist_ok=True)


def fetch_historical_data(brokerage_client, symbol, exchange, timeframe="1min"):
    """
    Fetch historical data for a specific timeframe.
    
    Args:
        brokerage_client: BrokerageClient instance
        symbol: Trading symbol
        exchange: Exchange name
        timeframe: Timeframe to fetch data for
        
    Returns:
        pd.DataFrame: Historical OHLC data
    """
    to_date = datetime.datetime.today()
    from_date = to_date - datetime.timedelta(days=DAYS)
    
    # For daily data, fetch more days to ensure we have enough data
    if timeframe == "day":
        days_to_fetch = max(DAYS * 2, 365)  # At least 1 year for daily data
        from_date = to_date - datetime.timedelta(days=days_to_fetch)
    
    logger.info(f"Fetching {timeframe} historical data for {symbol} ({exchange}) from {from_date.date()} to {to_date.date()}")
    
    data = brokerage_client.fetch_ohlc(symbol=symbol, exchange=exchange, interval=timeframe, duration=DAYS)
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning(f"No data returned for {symbol} ({exchange}) with {timeframe} interval")
        return df
    
    file_path = os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_{timeframe}_historical.csv")
    df.to_csv(file_path, index=False)
    logger.info(f"Saved {len(df)} records to {file_path}")
    return df


def apply_strategies(df, symbol, exchange, timeframe, quantity=100):
    """
    Apply MACD strategy to the given data.
    
    Args:
        df: DataFrame with OHLC data
        symbol: Trading symbol
        exchange: Exchange name
        timeframe: Timeframe description
        quantity: Number of shares to trade
        
    Returns:
        Tuple of (trades_df, total_profit, strategy_stats)
    """
    logger.info(f"Running MACD strategy on {symbol} ({exchange}) - {timeframe} with quantity = {quantity}")
    
    try:
        macd_strategy = MACDStrategy(df, quantity=quantity)
        trades_df, total_profit = macd_strategy.calculate_profit()
        strategy_stats = macd_strategy.get_strategy_stats()
        
        # Save plot for this timeframe
        plot_path = os.path.join(REPORT_DUMP_FOLDER, f"macd_{symbol}_{timeframe.replace(' ', '_')}_signals.png")
        macd_strategy.plot_signals(save_path=plot_path, show_plot=False)
        
        # Save signals to CSV
        signals_path = os.path.join(REPORT_DUMP_FOLDER, f"macd_{symbol}_{timeframe.replace(' ', '_')}_signals.csv")
        macd_strategy.save_signals_to_csv(signals_path)
        
        return trades_df, total_profit, strategy_stats
        
    except Exception as e:
        logger.error(f"Failed to apply strategy for {timeframe}: {e}")
        return pd.DataFrame(), 0.0, {}


def resample_data(df, timeframe_key):
    """
    Resample 1-minute data to the desired timeframe.
    
    Args:
        df: DataFrame with 1-minute OHLC data
        timeframe_key: Key from TIMEFRAMES dict
        
    Returns:
        pd.DataFrame: Resampled data
    """
    if timeframe_key not in TIMEFRAMES:
        raise ValueError(f"Unknown timeframe: {timeframe_key}")
    
    config = TIMEFRAMES[timeframe_key]
    
    # For daily data, use different resampling
    if config["resample_factor"] is None:
        # Resample to daily bars
        df_resampled = df.resample('D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
    else:
        # Resample to custom minute intervals
        df_resampled = df.resample(f'{config["resample_factor"]}T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
    
    logger.info(f"Resampled data from {len(df)} to {len(df_resampled)} bars for {config['description']}")
    return df_resampled


def load_local_data(symbol, exchange, timeframe="1m"):
    """
    Load local historical data from CSV file.
    
    Strategy:
    1. First look for exact timeframe match (e.g., NSE_ITI_30m.csv)
    2. If not found, try to aggregate from smaller timeframes
    3. If insufficient data, return None with clear message
    
    Args:
        symbol: Trading symbol
        exchange: Exchange name
        timeframe: Timeframe of the data (e.g., "1m", "30m", "1d")
        
    Returns:
        pd.DataFrame: Historical data or None if not found
    """
    # Step 1: Try exact timeframe match first
    exact_path = os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_{timeframe}.csv")
    if os.path.exists(exact_path):
        logger.info(f"Found exact timeframe file: {exact_path}")
        return _load_csv_file(exact_path)
    
    logger.info(f"Exact timeframe file not found: {exact_path}")
    
    # Step 2: Try to aggregate from smaller timeframes
    if timeframe == "10m":
        return _aggregate_to_10m(symbol, exchange)
    elif timeframe == "30m":
        return _aggregate_to_30m(symbol, exchange)
    elif timeframe == "1h":
        return _aggregate_to_1h(symbol, exchange)
    elif timeframe == "3h":
        return _aggregate_to_3h(symbol, exchange)
    elif timeframe == "1d":
        return _aggregate_to_1d(symbol, exchange)
    else:
        # Fallback to old logic for other timeframes
        possible_paths = [
            os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_{timeframe}_historical.csv"),
            os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"),
            os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1d.csv")
        ]
        
        file_path = None
        for path in possible_paths:
            if os.path.exists(path):
                file_path = path
                break
        
        if file_path is None:
            logger.warning(f"File not found. Tried: {possible_paths}")
            return None
        else:
            return _load_csv_file(file_path)


def _load_csv_file(file_path):
    """Load and process a CSV file."""
    try:
        # Try to read with headers first
        df = pd.read_csv(file_path)
        
        # If no headers or only 2 columns, assume it's date,close format
        if len(df.columns) == 2 and not any(col in df.columns for col in ['open', 'high', 'low', 'close']):
            df.columns = ['date', 'close']
            # Create OHLC data from close price (simple approximation)
            df['open'] = df['close'].shift(1).fillna(df['close'])
            df['high'] = df[['open', 'close']].max(axis=1)
            df['low'] = df[['open', 'close']].min(axis=1)
            df['volume'] = 1000  # Default volume
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        
        logger.info(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from {file_path}: {e}")
        return None


def _aggregate_to_10m(symbol, exchange):
    """Aggregate 1-minute data to 10-minute bars."""
    logger.info(f"Attempting to aggregate 1m data to 10m for {symbol}")
    
    # Try to load 1-minute data
    df_1m = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
    if df_1m is None:
        logger.error(f"❌ INSUFFICIENT DATA: Cannot find 1m data file for {symbol} to aggregate to 10m")
        return None
    
    if len(df_1m) < 10:
        logger.error(f"❌ INSUFFICIENT DATA: Only {len(df_1m)} 1m bars available, need at least 10 to create 10m bars")
        return None
    
    # Resample to 10 minutes
    df_10m = df_1m.resample('10min').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    logger.info(f"✅ Successfully aggregated {len(df_1m)} 1m bars to {len(df_10m)} 10m bars")
    return df_10m


def _aggregate_to_30m(symbol, exchange):
    """Aggregate 1-minute data to 30-minute bars."""
    logger.info(f"Attempting to aggregate 1m data to 30m for {symbol}")
    
    # Try to load 1-minute data
    df_1m = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
    if df_1m is None:
        logger.error(f"❌ INSUFFICIENT DATA: Cannot find 1m data file for {symbol} to aggregate to 30m")
        return None
    
    if len(df_1m) < 30:
        logger.error(f"❌ INSUFFICIENT DATA: Only {len(df_1m)} 1m bars available, need at least 30 to create 30m bars")
        return None
    
    # Resample to 30 minutes
    df_30m = df_1m.resample('30min').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    logger.info(f"✅ Successfully aggregated {len(df_1m)} 1m bars to {len(df_30m)} 30m bars")
    return df_30m


def _aggregate_to_1h(symbol, exchange):
    """Aggregate smaller timeframe data to 1-hour bars."""
    logger.info(f"Attempting to aggregate data to 1h for {symbol}")
    
    # Try 1m data first
    df_1m = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
    if df_1m is not None and len(df_1m) >= 60:
        df_1h = df_1m.resample('1H').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        logger.info(f"✅ Successfully aggregated {len(df_1m)} 1m bars to {len(df_1h)} 1h bars")
        return df_1h
    
    logger.error(f"❌ INSUFFICIENT DATA: Need at least 60 1m bars to create 1h bars, got {len(df_1m) if df_1m is not None else 0}")
    return None


def _aggregate_to_3h(symbol, exchange):
    """Aggregate smaller timeframe data to 3-hour bars."""
    logger.info(f"Attempting to aggregate data to 3h for {symbol}")
    
    # Try 1m data first
    df_1m = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
    if df_1m is not None and len(df_1m) >= 180:
        df_3h = df_1m.resample('3H').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        logger.info(f"✅ Successfully aggregated {len(df_1m)} 1m bars to {len(df_3h)} 3h bars")
        return df_3h
    
    logger.error(f"❌ INSUFFICIENT DATA: Need at least 180 1m bars to create 3h bars, got {len(df_1m) if df_1m is not None else 0}")
    return None


def _aggregate_to_1d(symbol, exchange):
    """Aggregate smaller timeframe data to 1-day bars."""
    logger.info(f"Attempting to aggregate data to 1d for {symbol}")
    
    # Try 1m data first
    df_1m = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
    if df_1m is not None and len(df_1m) >= 1440:  # 24 hours * 60 minutes
        df_1d = df_1m.resample('1D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        logger.info(f"✅ Successfully aggregated {len(df_1m)} 1m bars to {len(df_1d)} 1d bars")
        return df_1d
    
    # Fallback to existing 1d file
    df_1d = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1d.csv"))
    if df_1d is not None:
        logger.info(f"✅ Using existing 1d data file with {len(df_1d)} bars")
        return df_1d
    
    logger.error(f"❌ INSUFFICIENT DATA: Cannot create 1d bars from {len(df_1m) if df_1m is not None else 0} 1m bars and no 1d file found")
    return None


def run_multi_timeframe_analysis(symbol, exchange, use_live_data=False):
    """
    Run MACD strategy analysis across multiple timeframes.
    
    Args:
        symbol: Trading symbol
        exchange: Exchange name
        use_live_data: Whether to fetch fresh data from API
        
    Returns:
        Dict: Results for each timeframe
    """
    results = {}
    
    # First, get the base 1-minute data
    if use_live_data:
        brokerage_client = BrokerageClient()
        try:
            brokerage_client.fetch_and_cache_instruments(exchange)
            instrument_token = int(brokerage_client.get_instrument_token(symbol, exchange))
            
            if instrument_token == -1:
                logger.error(f"Instrument token not found for {exchange}:{symbol}")
                return results
            
            base_df = fetch_historical_data(brokerage_client, symbol, exchange, "minute")
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return results
    else:
        # Load 1-minute data directly for base_df
        base_df = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
        if base_df is None:
            logger.error(f"Could not load 1m base data for {symbol}")
            return results
    
    if base_df.empty:
        logger.error("No base data available")
        return results
    
    # Ensure date column is properly set as index
    if 'date' in base_df.columns:
        base_df['date'] = pd.to_datetime(base_df['date'])
        base_df.set_index('date', inplace=True)
    
    logger.info(f"Starting multi-timeframe analysis for {symbol} ({exchange})")
    logger.info(f"Base data: {len(base_df)} records from {base_df.index.min()} to {base_df.index.max()}")
    
    # Run analysis for each timeframe
    for timeframe_key, config in TIMEFRAMES.items():
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Analyzing {config['description']} timeframe")
            logger.info(f"{'='*50}")
            
            # Load data for this timeframe using new logic
            if timeframe_key == "1m":
                df_timeframe = load_local_data(symbol, exchange, "1m")
            elif timeframe_key == "10m":
                df_timeframe = load_local_data(symbol, exchange, "10m")
            elif timeframe_key == "30m":
                df_timeframe = load_local_data(symbol, exchange, "30m")
            elif timeframe_key == "1h":
                df_timeframe = load_local_data(symbol, exchange, "1h")
            elif timeframe_key == "3h":
                df_timeframe = load_local_data(symbol, exchange, "3h")
            elif timeframe_key == "1d":
                df_timeframe = load_local_data(symbol, exchange, "1d")
            else:
                # Fallback to old resampling logic
                df_timeframe = resample_data(base_df, timeframe_key)
            
            if df_timeframe.empty:
                logger.warning(f"No data available for {config['description']}")
                continue
            
            # Apply strategy
            trades_df, total_profit, strategy_stats = apply_strategies(
                df_timeframe, symbol, exchange, config['description']
            )
            
            # Store results
            results[timeframe_key] = {
                'timeframe': config['description'],
                'data_points': len(df_timeframe),
                'trades_df': trades_df,
                'total_profit': total_profit,
                'strategy_stats': strategy_stats,
                'data_period': f"{df_timeframe.index.min()} to {df_timeframe.index.max()}"
            }
            
            # Print summary
            if trades_df.empty:
                logger.info(f"No trades triggered for {config['description']}")
            else:
                logger.info(f"Completed {len(trades_df)} trades with total profit: ₹{total_profit:.2f}")
                logger.info(f"Win rate: {strategy_stats.get('win_rate', 0):.1f}%")
                
        except Exception as e:
            logger.error(f"Error analyzing {config['description']}: {e}")
            results[timeframe_key] = {
                'timeframe': config['description'],
                'error': str(e)
            }
    
    return results


def print_comprehensive_results(results):
    """
    Print comprehensive results comparison across all timeframes.
    
    Args:
        results: Dictionary of results from run_multi_timeframe_analysis
    """
    print(f"\n{'='*80}")
    print("COMPREHENSIVE MULTI-TIMEFRAME ANALYSIS RESULTS")
    print(f"{'='*80}")
    
    # Create summary table
    summary_data = []
    for timeframe_key, result in results.items():
        if 'error' in result:
            summary_data.append({
                'Timeframe': result['timeframe'],
                'Data Points': 'Error',
                'Total Trades': 'Error',
                'Total Profit (₹)': 'Error',
                'Win Rate (%)': 'Error',
                'Avg Profit/Trade (₹)': 'Error'
            })
        else:
            stats = result['strategy_stats']
            summary_data.append({
                'Timeframe': result['timeframe'],
                'Data Points': result['data_points'],
                'Total Trades': stats.get('total_trades', 0),
                'Total Profit (₹)': f"{result['total_profit']:.2f}",
                'Win Rate (%)': f"{stats.get('win_rate', 0):.1f}",
                'Avg Profit/Trade (₹)': f"{stats.get('avg_profit_per_trade', 0):.2f}"
            })
    
    # Print summary table
    summary_df = pd.DataFrame(summary_data)
    print("\nSUMMARY TABLE:")
    print(summary_df.to_string(index=False))
    
    # Find best performing timeframe
    valid_results = {k: v for k, v in results.items() if 'error' not in v}
    if valid_results:
        best_timeframe = max(valid_results.items(), key=lambda x: x[1]['total_profit'])
        print(f"\n🏆 BEST PERFORMING TIMEFRAME: {best_timeframe[1]['timeframe']}")
        print(f"   Total Profit: ₹{best_timeframe[1]['total_profit']:.2f}")
        print(f"   Win Rate: {best_timeframe[1]['strategy_stats'].get('win_rate', 0):.1f}%")
        print(f"   Total Trades: {best_timeframe[1]['strategy_stats'].get('total_trades', 0)}")
    
    print(f"\n{'='*80}")
    print("Detailed results and plots saved in the 'backtesting/report_dump' folder")
    print(f"{'='*80}")


def main():
    """
    Main function to run multi-timeframe backtesting analysis.
    """
    ensure_data_folder()
    ensure_report_dump_folder()

    # Configuration
    use_live_data = False  # Set to True to fetch fresh data from API
    exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    
    logger.info(f"Starting multi-timeframe backtesting for {symbol} ({exchange})")
    logger.info(f"Using {'live' if use_live_data else 'local'} data")
    
    # Run multi-timeframe analysis
    results = run_multi_timeframe_analysis(symbol, exchange, use_live_data)
    
    if not results:
        logger.error("No results generated. Please check your data and configuration.")
        return
    
    # Print comprehensive results
    print_comprehensive_results(results)
    
    # Save detailed results to CSV
    save_detailed_results(results, symbol, exchange)


def save_detailed_results(results, symbol, exchange):
    """
    Save detailed results to CSV files for further analysis.
    
    Args:
        results: Results dictionary from multi-timeframe analysis
        symbol: Trading symbol
        exchange: Exchange name
    """
    try:
        # Save summary results
        summary_data = []
        for timeframe_key, result in results.items():
            if 'error' not in result:
                stats = result['strategy_stats']
                summary_data.append({
                    'timeframe': result['timeframe'],
                    'data_points': result['data_points'],
                    'total_trades': stats.get('total_trades', 0),
                    'total_profit': result['total_profit'],
                    'win_rate': stats.get('win_rate', 0),
                    'avg_profit_per_trade': stats.get('avg_profit_per_trade', 0),
                    'max_profit': stats.get('max_profit', 0),
                    'max_loss': stats.get('max_loss', 0),
                    'winning_trades': stats.get('winning_trades', 0),
                    'losing_trades': stats.get('losing_trades', 0),
                    'data_period': result['data_period']
                })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_path = os.path.join(REPORT_DUMP_FOLDER, f"multi_timeframe_summary_{symbol}.csv")
            summary_df.to_csv(summary_path, index=False)
            logger.info(f"Summary results saved to: {summary_path}")
        
        # Save individual trade details for each timeframe
        for timeframe_key, result in results.items():
            if 'error' not in result and not result['trades_df'].empty:
                trades_path = os.path.join(REPORT_DUMP_FOLDER, f"trades_{symbol}_{timeframe_key}.csv")
                result['trades_df'].to_csv(trades_path, index=False)
                logger.info(f"Trade details for {result['timeframe']} saved to: {trades_path}")
                
    except Exception as e:
        logger.error(f"Failed to save detailed results: {e}")


def run_single_timeframe(timeframe_key, symbol=None, exchange=None, use_live_data=False):
    """
    Run analysis for a single specific timeframe.
    
    Args:
        timeframe_key: Key from TIMEFRAMES dict (e.g., '10min', '1hr', '1d')
        symbol: Trading symbol (uses config default if None)
        exchange: Exchange name (uses config default if None)
        use_live_data: Whether to fetch fresh data
        
    Returns:
        Dict: Results for the specified timeframe
    """
    if timeframe_key not in TIMEFRAMES:
        raise ValueError(f"Unknown timeframe: {timeframe_key}. Available: {list(TIMEFRAMES.keys())}")
    
    if symbol is None or exchange is None:
        exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    
    # Ensure folders exist
    ensure_data_folder()
    ensure_report_dump_folder()
    
    logger.info(f"Running single timeframe analysis: {TIMEFRAMES[timeframe_key]['description']}")
    
    # Get base data
    if use_live_data:
        brokerage_client = BrokerageClient()
        try:
            brokerage_client.fetch_and_cache_instruments(exchange)
            instrument_token = int(brokerage_client.get_instrument_token(symbol, exchange))
            
            if instrument_token == -1:
                logger.error(f"Instrument token not found for {exchange}:{symbol}")
                return {}
            
            base_df = fetch_historical_data(brokerage_client, symbol, exchange, "minute")
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return {}
    else:
        # Load 1-minute data directly for base_df
        base_df = _load_csv_file(os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_1m.csv"))
        if base_df is None:
            logger.error(f"Could not load 1m base data for {symbol}")
            return {}
    
    if base_df.empty:
        logger.error("No base data available")
        return {}
    
    # Ensure date column is properly set as index
    if 'date' in base_df.columns:
        base_df['date'] = pd.to_datetime(base_df['date'])
        base_df.set_index('date', inplace=True)
    
    config = TIMEFRAMES[timeframe_key]
    
    # Load data for this timeframe using new logic
    if timeframe_key == "1m":
        df_timeframe = load_local_data(symbol, exchange, "1m")
    elif timeframe_key == "10m":
        df_timeframe = load_local_data(symbol, exchange, "10m")
    elif timeframe_key == "30m":
        df_timeframe = load_local_data(symbol, exchange, "30m")
    elif timeframe_key == "1h":
        df_timeframe = load_local_data(symbol, exchange, "1h")
    elif timeframe_key == "3h":
        df_timeframe = load_local_data(symbol, exchange, "3h")
    elif timeframe_key == "1d":
        df_timeframe = load_local_data(symbol, exchange, "1d")
    else:
        # Fallback to old resampling logic
        df_timeframe = resample_data(base_df, timeframe_key)
    
    if df_timeframe.empty:
        logger.warning(f"No data available for {config['description']}")
        return {}
    
    # Apply strategy
    trades_df, total_profit, strategy_stats = apply_strategies(
        df_timeframe, symbol, exchange, config['description']
    )
    
    result = {
        'timeframe': config['description'],
        'data_points': len(df_timeframe),
        'trades_df': trades_df,
        'total_profit': total_profit,
        'strategy_stats': strategy_stats,
        'data_period': f"{df_timeframe.index.min()} to {df_timeframe.index.max()}"
    }
    
    # Print results
    if trades_df.empty:
        logger.info(f"No trades triggered for {config['description']}")
    else:
        logger.info(f"Completed {len(trades_df)} trades with total profit: ₹{total_profit:.2f}")
        logger.info(f"Win rate: {strategy_stats.get('win_rate', 0):.1f}%")
    
    return {timeframe_key: result}


if __name__ == "__main__":
    main()
