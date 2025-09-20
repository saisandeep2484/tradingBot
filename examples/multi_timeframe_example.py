#!/usr/bin/env python3
"""
Multi-Timeframe Backtesting Example

This script demonstrates how to use the enhanced backtest engine
to analyze MACD strategy across multiple timeframes.

Usage:
    python examples/multi_timeframe_example.py
"""

import sys
import os

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtesting.backtest_engine import (
    run_multi_timeframe_analysis,
    run_single_timeframe,
    print_comprehensive_results,
    TIMEFRAMES
)
from config.config import INTRADAY_TARGET_STOCK


def main():
    """
    Example usage of multi-timeframe backtesting.
    """
    print("=" * 80)
    print("MULTI-TIMEFRAME BACKTESTING EXAMPLE")
    print("=" * 80)
    
    # Parse target stock from config
    exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    print(f"Target Stock: {symbol} ({exchange})")
    print(f"Available Timeframes: {list(TIMEFRAMES.keys())}")
    print()
    
    # Example 1: Run analysis for all timeframes
    print("Example 1: Running analysis for ALL timeframes")
    print("-" * 50)
    
    results = run_multi_timeframe_analysis(
        symbol=symbol,
        exchange=exchange,
        use_live_data=False  # Use local CSV data
    )
    
    if results:
        print_comprehensive_results(results)
    else:
        print("No results generated. Make sure you have historical data available.")
    
    print("\n" + "=" * 80)
    
    # Example 2: Run analysis for a single timeframe
    print("Example 2: Running analysis for a SINGLE timeframe (1 Hour)")
    print("-" * 50)
    
    single_result = run_single_timeframe(
        timeframe_key="1h",
        symbol=symbol,
        exchange=exchange,
        use_live_data=False
    )
    
    if single_result:
        print(f"Results for 1 Hour timeframe:")
        result = single_result["1h"]
        print(f"  Data Points: {result['data_points']}")
        print(f"  Total Profit: ₹{result['total_profit']:.2f}")
        print(f"  Win Rate: {result['strategy_stats'].get('win_rate', 0):.1f}%")
        print(f"  Total Trades: {result['strategy_stats'].get('total_trades', 0)}")
    
    print("\n" + "=" * 80)
    print("Example completed!")
    print("Check the 'stock_data' folder for detailed results and plots.")
    print("=" * 80)


def demonstrate_timeframe_comparison():
    """
    Demonstrate how to compare specific timeframes.
    """
    print("\n" + "=" * 80)
    print("TIMEFRAME COMPARISON EXAMPLE")
    print("=" * 80)
    
    exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    
    # Compare short-term vs long-term timeframes
    short_term_timeframes = ["10m", "30m", "1h"]
    long_term_timeframes = ["3h", "1d"]
    
    print("Comparing Short-term vs Long-term timeframes:")
    print(f"Short-term: {short_term_timeframes}")
    print(f"Long-term: {long_term_timeframes}")
    print()
    
    # Run analysis for short-term timeframes
    short_term_results = {}
    for tf in short_term_timeframes:
        result = run_single_timeframe(tf, symbol, exchange, use_live_data=False)
        if result:
            short_term_results.update(result)
    
    # Run analysis for long-term timeframes
    long_term_results = {}
    for tf in long_term_timeframes:
        result = run_single_timeframe(tf, symbol, exchange, use_live_data=False)
        if result:
            long_term_results.update(result)
    
    # Compare results
    print("SHORT-TERM TIMEFRAMES SUMMARY:")
    print("-" * 40)
    for tf_key, result in short_term_results.items():
        if 'error' not in result:
            print(f"{result['timeframe']:15} | Profit: ₹{result['total_profit']:8.2f} | "
                  f"Trades: {result['strategy_stats'].get('total_trades', 0):3d} | "
                  f"Win Rate: {result['strategy_stats'].get('win_rate', 0):5.1f}%")
    
    print("\nLONG-TERM TIMEFRAMES SUMMARY:")
    print("-" * 40)
    for tf_key, result in long_term_results.items():
        if 'error' not in result:
            print(f"{result['timeframe']:15} | Profit: ₹{result['total_profit']:8.2f} | "
                  f"Trades: {result['strategy_stats'].get('total_trades', 0):3d} | "
                  f"Win Rate: {result['strategy_stats'].get('win_rate', 0):5.1f}%")
    
    # Find best performing timeframe in each category
    if short_term_results:
        best_short = max(short_term_results.items(), 
                        key=lambda x: x[1]['total_profit'] if 'error' not in x[1] else -float('inf'))
        print(f"\nBest Short-term: {best_short[1]['timeframe']} (₹{best_short[1]['total_profit']:.2f})")
    
    if long_term_results:
        best_long = max(long_term_results.items(), 
                       key=lambda x: x[1]['total_profit'] if 'error' not in x[1] else -float('inf'))
        print(f"Best Long-term: {best_long[1]['timeframe']} (₹{best_long[1]['total_profit']:.2f})")


if __name__ == "__main__":
    try:
        main()
        
        # Uncomment the line below to run the comparison example
        # demonstrate_timeframe_comparison()
        
    except Exception as e:
        print(f"Error running example: {e}")
        print("Make sure you have:")
        print("1. Historical data in the 'stock_data' folder")
        print("2. Proper configuration in 'config/config.py'")
        print("3. All required dependencies installed")
