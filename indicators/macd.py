"""
MACD (Moving Average Convergence Divergence) Indicator

This module implements the MACD technical indicator for real-time calculation.
"""

import time
from typing import Optional, Tuple
from datetime import datetime

from .base import BaseIndicator


class MACDIndicator(BaseIndicator):
    """
    MACD Indicator for real-time calculation.
    
    The MACD (Moving Average Convergence Divergence) is a trend-following 
    momentum indicator that shows the relationship between two moving averages 
    of a security's price.
    """
    
    def __init__(self, 
                 exchange: str,
                 ticker: str,
                 fast_period: int = 12,
                 slow_period: int = 26,
                 signal_period: int = 9):
        """
        Initialize MACD Indicator for real-time calculation.

        Args:
            exchange: Exchange name (e.g., 'NSE')
            ticker: Stock ticker (e.g., 'ITI')
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period
        """
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        
        # Calculate alpha values for EMA
        self.fast_alpha = 2 / (fast_period + 1)
        self.slow_alpha = 2 / (slow_period + 1)
        self.signal_alpha = 2 / (signal_period + 1)
        
        super().__init__(exchange, ticker, 
                        fast_period=fast_period, 
                        slow_period=slow_period, 
                        signal_period=signal_period)
    
    def _get_indicator_name(self) -> str:
        """Return the name of the indicator for file naming."""
        return "macd"
    
    def _initialize_indicator_data(self, **kwargs):
        """Initialize MACD-specific data structures."""
        self.macd_line = []
        self.signal_line = []
        self.histogram = []
        
        self.fast_ema = None
        self.slow_ema = None
        self.signal_ema = None
    
    def _initialize_output_file(self):
        """Initialize the output file with MACD headers."""
        with open(self.output_file, 'w') as f:
            f.write("timestamp,price,macd_line,signal_line,histogram\n")
    
    def _calculate_ema(self, price: float, prev_ema: Optional[float], alpha: float) -> float:
        """Calculate Exponential Moving Average."""
        return price if prev_ema is None else alpha * price + (1 - alpha) * prev_ema

    def _calculate_sma(self, values: list, period: int) -> float:
        """Calculate Simple Moving Average."""
        return sum(values[-period:]) / period
    
    def _process_new_price(self, timestamp: datetime, price: float) -> Tuple[float, float, float]:
        """Process a new price point and return MACD values."""
        self.timestamps.append(timestamp)
        self.prices.append(price)

        if len(self.prices) == 1:
            # First price point
            self.fast_ema = price
            self.slow_ema = price
            macd_value = 0
            signal_value = 0
        elif len(self.prices) <= self.slow_period:
            # Building up to slow period
            self.fast_ema = self._calculate_sma(self.prices, self.fast_period) if len(self.prices) >= self.fast_period else self._calculate_ema(price, self.fast_ema, self.fast_alpha)
            self.slow_ema = self._calculate_sma(self.prices, self.slow_period) if len(self.prices) == self.slow_period else self._calculate_ema(price, self.slow_ema, self.slow_alpha)
            macd_value = self.fast_ema - self.slow_ema
            signal_value = self.signal_ema if self.signal_ema is not None else macd_value
        else:
            # Full EMA calculation
            self.fast_ema = self._calculate_ema(price, self.fast_ema, self.fast_alpha)
            self.slow_ema = self._calculate_ema(price, self.slow_ema, self.slow_alpha)
            macd_value = self.fast_ema - self.slow_ema

            # Calculate signal line
            if len(self.macd_line) >= self.signal_period:
                self.signal_ema = self._calculate_ema(macd_value, self.signal_ema, self.signal_alpha)
                signal_value = self.signal_ema
            else:
                temp_macd = self.macd_line + [macd_value]
                if len(temp_macd) >= self.signal_period:
                    signal_value = sum(temp_macd[-self.signal_period:]) / self.signal_period
                    self.signal_ema = signal_value
                else:
                    signal_value = self.signal_ema if self.signal_ema is not None else macd_value

        histogram_value = macd_value - signal_value

        # Store values
        self.macd_line.append(macd_value)
        self.signal_line.append(signal_value)
        self.histogram.append(histogram_value)

        return macd_value, signal_value, histogram_value
    
    def _print_update(self, timestamp: datetime, price: float, values: Tuple[float, float, float]):
        """Print MACD-specific update."""
        macd_value, signal_value, histogram_value = values
        print(f"{timestamp}: Price={price:.2f}, MACD={macd_value:.4f}, Signal={signal_value:.4f}, Histogram={histogram_value:.4f}")
    
    def get_latest_values(self) -> Optional[Tuple[float, float, float]]:
        """Get the latest MACD values."""
        if not self.macd_line:
            return None
        return (self.macd_line[-1], self.signal_line[-1], self.histogram[-1])
    
    def start_streaming(self, interval: float = 1.0):
        """Start streaming MACD calculations."""
        print(f"Starting MACD streaming for {self.ticker} on {self.exchange}")
        print(f"Input: {self.input_file}")
        print(f"Output: {self.output_file}")
        print(f"MACD Params: Fast={self.fast_period}, Slow={self.slow_period}, Signal={self.signal_period}")
        print("Press Ctrl+C to stop\n")

        self.update()

        try:
            while True:
                time.sleep(interval)
                new_count = self.update()
                if new_count == 0:
                    print(".", end="", flush=True)
        except KeyboardInterrupt:
            print("\nStopping MACD streamer...")

    def process_existing_data(self):
        """Process all existing data in the input file."""
        print("Processing existing data...")
        count = self.update()
        print(f"Processed {count} records")

        if count > 0:
            latest = self.get_latest_values()
            if latest:
                print(f"Latest MACD values - MACD: {latest[0]:.4f}, Signal: {latest[1]:.4f}, Histogram: {latest[2]:.4f}")
