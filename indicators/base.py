"""
Base Indicator Class

This module provides a base class for all technical indicators with common functionality.
"""

from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any
from datetime import datetime
import os


class BaseIndicator(ABC):
    """
    Abstract base class for all technical indicators.
    
    Provides common functionality for file handling, data processing, and output management.
    """
    
    def __init__(self, exchange: str, ticker: str, **kwargs):
        """
        Initialize base indicator.
        
        Args:
            exchange: Exchange name (e.g., 'NSE')
            ticker: Stock ticker (e.g., 'ITI')
            **kwargs: Additional indicator-specific parameters
        """
        self.exchange = exchange.upper()
        self.ticker = ticker.upper()
        
        # Set up file paths
        self._setup_file_paths()
        
        # Initialize data storage
        self.prices = []
        self.timestamps = []
        self.last_processed_line = 0
        
        # Create output directory
        os.makedirs("strategy_data", exist_ok=True)
        
        # Initialize indicator-specific data
        self._initialize_indicator_data(**kwargs)
        
        # Initialize output file
        self._initialize_output_file()
    
    def _setup_file_paths(self):
        """Set up input and output file paths."""
        filename = f"{self.exchange}_{self.ticker}_1minute.csv"
        self.input_file = f"stock_data/{filename}"
        self.output_file = f"strategy_data/{self._get_indicator_name()}_{filename}"
    
    @abstractmethod
    def _get_indicator_name(self) -> str:
        """Return the name of the indicator for file naming."""
        pass
    
    @abstractmethod
    def _initialize_indicator_data(self, **kwargs):
        """Initialize indicator-specific data structures."""
        pass
    
    @abstractmethod
    def _initialize_output_file(self):
        """Initialize the output file with headers."""
        pass
    
    @abstractmethod
    def _process_new_price(self, timestamp: datetime, price: float) -> Tuple[Any, ...]:
        """Process a new price point and return indicator values."""
        pass
    
    def _read_new_data(self) -> list:
        """Read new data from the input file."""
        new_data = []
        try:
            with open(self.input_file, 'r') as f:
                lines = f.readlines()

            for i in range(self.last_processed_line, len(lines)):
                line = lines[i].strip()
                if line and ',' in line:
                    try:
                        timestamp_str, price_str = line.split(',')
                        timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
                        price = float(price_str.strip())
                        new_data.append((timestamp, price))
                    except ValueError:
                        continue

            self.last_processed_line = len(lines)

        except FileNotFoundError:
            print(f"Input file {self.input_file} not found")

        return new_data
    
    def _write_to_output(self, timestamp: datetime, price: float, *values):
        """Write indicator values to output file."""
        with open(self.output_file, 'a') as f:
            values_str = ','.join([f"{v:.4f}" if isinstance(v, (int, float)) else str(v) for v in values])
            f.write(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')},{price:.2f},{values_str}\n")
    
    def update(self):
        """Update indicator with new data."""
        new_data = self._read_new_data()

        for timestamp, price in new_data:
            values = self._process_new_price(timestamp, price)
            self._write_to_output(timestamp, price, *values)
            
            # Print update (can be overridden by subclasses)
            self._print_update(timestamp, price, values)

        return len(new_data)
    
    def _print_update(self, timestamp: datetime, price: float, values: Tuple[Any, ...]):
        """Print indicator update (can be overridden by subclasses)."""
        values_str = ', '.join([f"{v:.4f}" if isinstance(v, (int, float)) else str(v) for v in values])
        print(f"{timestamp}: Price={price:.2f}, {self._get_indicator_name().upper()}={values_str}")
    
    @abstractmethod
    def get_latest_values(self) -> Optional[Tuple[Any, ...]]:
        """Get the latest indicator values."""
        pass
