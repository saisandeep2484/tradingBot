#!/usr/bin/env python3
"""
Quick test script to run multi-timeframe analysis and show results
"""

from backtesting.backtest_engine import run_multi_timeframe_analysis, print_comprehensive_results, save_detailed_results
from config.config import INTRADAY_TARGET_STOCK

def main():
    print("Running quick multi-timeframe analysis...")
    
    # Parse target stock
    exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    print(f"Analyzing: {symbol} ({exchange})")
    
    # Run analysis
    results = run_multi_timeframe_analysis(symbol, exchange, use_live_data=False)
    
    if results:
        print_comprehensive_results(results)
        # Save detailed results to CSV files
        save_detailed_results(results, symbol, exchange)
        print(f"\n✅ Analysis complete! Check the 'backtesting/report_dump' folder for detailed files.")
    else:
        print("❌ No results generated. Make sure you have historical data available.")

if __name__ == "__main__":
    main()
