import pandas as pd  # type: ignore
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Any
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger(__name__)


class MarketDataLogger:
    """
    Handles logging of market price data in CSV format.
    Provides functionality to log historical and live price data with proper error handling.
    """
    
    # Constants
    DEFAULT_OUTPUT_DIR = "stock_data"
    PRICE_COLUMNS = ["time", "last_price"]
    CSV_ENCODING = "utf-8"
    
    def __init__(self, brokerage_client: Any, output_dir: Optional[str] = None):
        """
        Initialize the market data logger.
        
        Args:
            brokerage_client: The BrokerageClient instance for fetching OHLC data
            output_dir: Directory to store price logs. If None, uses default directory
        """
        self.brokerage_client = brokerage_client
        self.output_dir = Path(output_dir) if output_dir else Path(self.DEFAULT_OUTPUT_DIR)
        self._ensure_output_directory()
    
    # Public methods (alphabetically ordered)
    
    def append_live_price(self, exchange: str, symbol: str, last_price: float, 
                         timestamp: Optional[datetime] = None) -> bool:
        """
        Appends a single price point to the 1-minute CSV file.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            last_price: Last traded price
            timestamp: Timestamp for the price. If None, uses current time
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            ValueError: If invalid parameters provided
        """
        if not self._validate_price_inputs(exchange, symbol, last_price):
            raise ValueError("Invalid price logging parameters")
            
        file_path = self._get_price_file_path(symbol, exchange)
        timestamp = timestamp or datetime.now()
        
        try:
            price_data = {
                "time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "last_price": last_price
            }
            
            df = pd.DataFrame([price_data])
            
            with self._managed_csv_write(file_path, not file_path.exists()) as should_write_header:
                df.to_csv(file_path, index=False, mode='a', 
                         header=should_write_header, encoding=self.CSV_ENCODING)
            
            logger.debug("Appended live price for %s:%s - %.2f at %s", 
                        exchange, symbol, last_price, timestamp)
            return True
            
        except Exception as e:
            logger.error("Failed to append live price for %s:%s - %s", exchange, symbol, e)
            return False
    
    def get_output_directory(self) -> Path:
        """
        Get the current output directory path.
        
        Returns:
            Path: Current output directory
        """
        return self.output_dir
    
    def log_previous_day_prices(self, symbol: str, exchange: str) -> bool:
        """
        Fetches 1-minute OHLC data for the previous day and logs time + close price.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self._validate_symbol_exchange(symbol, exchange):
            return False
            
        try:
            logger.info("Fetching previous day prices for %s:%s", exchange, symbol)
            
            df = self.brokerage_client.fetch_ohlc(
                symbol=symbol,
                exchange=exchange,
                interval="minute",
                duration=2  # Fetch enough to include yesterday
            )
            
            if df.empty:
                logger.warning("No OHLC data available for %s:%s", exchange, symbol)
                return False
            
            target_date = (datetime.today() - timedelta(days=1)).date()
            df_filtered = df[df.index.date == target_date]
            
            if df_filtered.empty:
                logger.warning("No data for previous day (%s) for %s:%s", target_date, exchange, symbol)
                return False
            
            success = self._save_price_data(df_filtered, symbol, exchange, mode='w')
            
            if success:
                logger.info("Successfully logged %d previous day price points for %s:%s", 
                           len(df_filtered), exchange, symbol)
            
            return success
            
        except Exception as e:
            logger.error("Error logging previous day prices for %s:%s - %s", exchange, symbol, e)
            return False
    
    def log_today_prices(self, symbol: str, exchange: str) -> bool:
        """
        Fetches 1-minute OHLC data for today and logs time + close price.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self._validate_symbol_exchange(symbol, exchange):
            return False
            
        try:
            logger.info("Fetching today's prices for %s:%s", exchange, symbol)
            
            df = self.brokerage_client.fetch_ohlc(
                symbol=symbol,
                exchange=exchange,
                interval="minute",
                duration=1  # Only today
            )
            
            if df.empty:
                logger.warning("No OHLC data available for %s:%s today", exchange, symbol)
                return False
            
            target_date = datetime.today().date()
            df_filtered = df[df.index.date == target_date]
            
            if df_filtered.empty:
                logger.warning("No data for today (%s) for %s:%s", target_date, exchange, symbol)
                return False
            
            output_path = self._get_price_file_path(symbol, exchange)
            mode = 'a' if output_path.exists() else 'w'
            
            success = self._save_price_data(df_filtered, symbol, exchange, mode=mode)
            
            if success:
                logger.info("Successfully logged %d today's price points for %s:%s", 
                           len(df_filtered), exchange, symbol)
            
            return success
            
        except Exception as e:
            logger.error("Error logging today's prices for %s:%s - %s", exchange, symbol, e)
            return False
    
    # Private methods (alphabetically ordered)
    
    def _ensure_output_directory(self) -> None:
        """Ensure the output directory exists."""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.debug("Ensured output directory exists: %s", self.output_dir)
        except Exception as e:
            logger.error("Failed to create output directory %s: %s", self.output_dir, e)
            raise
    
    def _get_price_file_path(self, symbol: str, exchange: str) -> Path:
        """
        Get the file path for storing price data.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            
        Returns:
            Path: File path for the price data
        """
        filename = f"{symbol}_{exchange}_1minute.csv"
        return self.output_dir / filename
    
    @contextmanager
    def _managed_csv_write(self, file_path: Path, write_header: bool):
        """
        Context manager for CSV writing operations.
        
        Args:
            file_path: Path to the CSV file
            write_header: Whether to write CSV header
            
        Yields:
            bool: Whether to write header
        """
        try:
            yield write_header
        except Exception as e:
            logger.error("Error during CSV write operation for %s: %s", file_path, e)
            raise
    
    def _save_price_data(self, df: pd.DataFrame, symbol: str, exchange: str, mode: str = 'w') -> bool:
        """
        Save price data to CSV file.
        
        Args:
            df: DataFrame containing OHLC data
            symbol: Trading symbol
            exchange: Exchange name
            mode: File write mode ('w' for write, 'a' for append)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Prepare data for saving
            df_to_save = df[["close"]].copy()
            df_to_save.reset_index(inplace=True)
            
            # Handle timezone-aware datetime
            if hasattr(df_to_save["date"].dtype, 'tz') and df_to_save["date"].dt.tz is not None:
                df_to_save["date"] = df_to_save["date"].dt.tz_localize(None)
            
            df_to_save.rename(columns={"date": "time", "close": "last_price"}, inplace=True)
            
            output_path = self._get_price_file_path(symbol, exchange)
            write_header = mode == 'w' or not output_path.exists()
            
            df_to_save.to_csv(output_path, index=False, mode=mode, 
                             header=write_header, encoding=self.CSV_ENCODING)
            
            logger.debug("Saved %d price records to %s (mode: %s)", len(df_to_save), output_path, mode)
            return True
            
        except Exception as e:
            logger.error("Failed to save price data for %s:%s - %s", exchange, symbol, e)
            return False
    
    def _validate_price_inputs(self, exchange: str, symbol: str, last_price: float) -> bool:
        """
        Validate inputs for price logging.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            last_price: Price value
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not exchange or not isinstance(exchange, str):
            logger.error("Invalid exchange: %s", exchange)
            return False
            
        if not symbol or not isinstance(symbol, str):
            logger.error("Invalid symbol: %s", symbol)
            return False
            
        if not isinstance(last_price, (int, float)) or last_price <= 0:
            logger.error("Invalid price: %s", last_price)
            return False
            
        return True
    
    def _validate_symbol_exchange(self, symbol: str, exchange: str) -> bool:
        """
        Validate symbol and exchange parameters.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not symbol or not isinstance(symbol, str):
            logger.error("Invalid symbol: %s", symbol)
            return False
            
        if not exchange or not isinstance(exchange, str):
            logger.error("Invalid exchange: %s", exchange)
            return False
            
        return True