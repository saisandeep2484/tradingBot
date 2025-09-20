"""
Example usage of the new indicators package structure.

This example demonstrates how to use the MACD indicator with the new organized structure.
"""

from indicators import MACDIndicator


def main():
    """Example of using the MACD indicator."""
    
    # Create MACD indicator instance
    macd = MACDIndicator(
        exchange="NSE",
        ticker="ITI",
        fast_period=12,
        slow_period=26,
        signal_period=9
    )
    
    # Process existing data
    macd.process_existing_data()
    
    # Get latest values
    latest = macd.get_latest_values()
    if latest:
        macd_value, signal_value, histogram_value = latest
        print(f"\nLatest MACD Values:")
        print(f"MACD Line: {macd_value:.4f}")
        print(f"Signal Line: {signal_value:.4f}")
        print(f"Histogram: {histogram_value:.4f}")
        
        # Simple signal interpretation
        if macd_value > signal_value and histogram_value > 0:
            print("Signal: BULLISH (MACD above signal line)")
        elif macd_value < signal_value and histogram_value < 0:
            print("Signal: BEARISH (MACD below signal line)")
        else:
            print("Signal: NEUTRAL")


if __name__ == "__main__":
    main()
