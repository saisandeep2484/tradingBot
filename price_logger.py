import pandas as pd
from datetime import datetime, timedelta
import os
import logging
from typing import Optional

class PriceLogger:
    def __init__(self, kite_session, output_dir: str = "stock_data"):
        """
        Initialize the price logger.
        Args:
            kite_session: The KiteSessionWrapper instance for fetching OHLC data.
            output_dir (str): Directory to store price logs. Defaults to 'stock_data'.
        """
        self.kite_session = kite_session
        self.output_dir = output_dir
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create output directory {self.output_dir}: {e}")

    def log_previous_day_prices(self, symbol: str, exchange: str) -> None:
        """
        Fetches 1-minute OHLC data for the previous day and logs time + close price.
        Args:
            symbol (str): Trading symbol.
            exchange (str): Exchange name.
        """
        try:
            df = self.kite_session.fetch_ohlc(
                symbol=symbol,
                exchange=exchange,
                interval="minute",
                duration=2  # Fetch enough to include yesterday
            )
            if df.empty:
                logging.warning(f"No OHLC data for {symbol} on {exchange}")
                return
            target_date = (datetime.today() - timedelta(days=1)).date()
            df_filtered = df[df.index.date == target_date]
            df_to_save = df_filtered[["close"]].copy()
            df_to_save.reset_index(inplace=True)
            df_to_save["date"] = df_to_save["date"].dt.tz_localize(None)
            df_to_save.rename(columns={"date": "time", "close": "last_price"}, inplace=True)
            output_path = os.path.join(self.output_dir, f"{symbol}_{exchange}_1minute.csv")
            df_to_save.to_csv(output_path, index=False, mode='w', header=True)
            logging.info(f"Logged {len(df_to_save)} price points to {output_path}")
        except Exception as e:
            logging.error(f"Error logging previous day prices for {symbol} on {exchange}: {e}")

    def log_today_prices(self, symbol: str, exchange: str) -> None:
        """
        Fetches 1-minute OHLC data for today and logs time + close price.
        Args:
            symbol (str): Trading symbol.
            exchange (str): Exchange name.
        """
        try:
            df = self.kite_session.fetch_ohlc(
                symbol=symbol,
                exchange=exchange,
                interval="minute",
                duration=1  # Only today
            )
            if df.empty:
                logging.warning(f"No OHLC data for {symbol} on {exchange} today")
                return
            target_date = datetime.today().date()
            df_filtered = df[df.index.date == target_date]
            df_to_save = df_filtered[["close"]].copy()
            df_to_save.reset_index(inplace=True)
            df_to_save["date"] = df_to_save["date"].dt.tz_localize(None)
            df_to_save.rename(columns={"date": "time", "close": "last_price"}, inplace=True)
            output_path = os.path.join(self.output_dir, f"{symbol}_{exchange}_1minute.csv")
            df_to_save.to_csv(output_path, index=False, mode='a', header=not os.path.exists(output_path))
            logging.info(f"Logged {len(df_to_save)} price points to {output_path}")
        except Exception as e:
            logging.error(f"Error logging today's prices for {symbol} on {exchange}: {e}")

    def append_live_price(self, exchange: str, symbol: str, last_price: float, timestamp: Optional[datetime] = None) -> None:
        """
        Appends a single price point to the 1-minute CSV file.
        Args:
            exchange (str): Exchange name.
            symbol (str): Trading symbol.
            last_price (float): Last traded price.
            timestamp (datetime, optional): Timestamp for the price. If None, uses current time.
        """
        file_path = os.path.join(self.output_dir, f"{symbol}_{exchange}_1minute.csv")
        row = {"time": (timestamp or datetime.now()), "last_price": last_price}
        try:
            df = pd.DataFrame([row])
            df.to_csv(file_path, index=False, mode='a', header=not os.path.exists(file_path))
            logging.info(f"Appended live price: {row} -> {file_path}")
        except Exception as e:
            logging.error(f"Failed to append live price for {symbol} on {exchange}: {e}. Row: {row}")