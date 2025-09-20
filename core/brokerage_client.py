import datetime as dt
import pandas as pd  # type: ignore
import csv
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager
from pathlib import Path

from data_handlers.order_logger import OrderLogger
from kiteconnect import KiteConnect, KiteTicker  # type: ignore
from config.config import API_KEY, API_SECRET, REQUEST_TOKEN

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BrokerageClient:
    """
    Handles KiteConnect brokerage operations including session management, 
    instrument data, ticker streaming, and order placement.
    """
    
    # Constants
    DATA_DUMP_DIR = Path("data_dump")
    TICKER_CSV_HEADERS = [
        "tradable", "mode", "instrument_token", "last_price", "last_traded_quantity",
        "average_traded_price", "volume_traded", "total_buy_quantity", "total_sell_quantity",
        "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "change",
        "last_trade_time", "oi", "oi_day_high", "oi_day_low", "exchange_timestamp"
    ]
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the BrokerageClient.
        
        Args:
            data_dir: Optional custom data directory path
        """
        self.data_dir = Path(data_dir) if data_dir else self.DATA_DUMP_DIR
        self.data_dir.mkdir(exist_ok=True)
        
        self.kitesession: Optional[KiteConnect] = None
        self._last_logged_tick: Dict[Tuple[str, str], Tuple[pd.Timestamp, float]] = {}
        self.instrument_dfs: Dict[str, pd.DataFrame] = {}
        self.order_logger = OrderLogger(str(self.data_dir / "order_log.jsonl"))
        self._active_websockets: List[KiteTicker] = []
        
        self._initialize_session()

    # Public methods (alphabetically ordered)
    
    def cleanup(self) -> None:
        """Clean up resources and close connections."""
        logger.info("Cleaning up BrokerageClient resources")
        
        # Close all active websockets
        for kws in self._active_websockets:
            try:
                kws.close()
            except Exception as e:
                logger.warning("Error closing websocket: %s", e)
        
        self._active_websockets.clear()
        logger.info("Cleanup completed")

    def fetch_and_cache_instruments(self, exchange: str) -> bool:
        """
        Fetch and cache instrument data for the given exchange.
        
        Args:
            exchange: Exchange name (e.g., 'NSE', 'BSE')
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.is_session_active():
            logger.error("Cannot fetch instruments - session not active")
            return False
            
        try:
            logger.info("Fetching instruments for exchange: %s", exchange)
            instrument_dump = self.kitesession.instruments(exchange)
            instrument_df = pd.DataFrame(instrument_dump)
            
            if instrument_df.empty:
                logger.warning("No instruments found for exchange: %s", exchange)
                return False
                
            self.instrument_dfs[exchange] = instrument_df

            filename = self.data_dir / f"{exchange}_Instruments.csv"
            instrument_df.to_csv(filename, index=False, mode='w')

            logger.info("Cached %d instruments for %s to %s", len(instrument_df), exchange, filename)
            return True
            
        except Exception as e:
            logger.error("Failed to fetch instruments for %s: %s", exchange, e)
            return False

    def fetch_ohlc(self, symbol: str, exchange: str, interval: str, duration: int) -> pd.DataFrame:
        """
        Fetch OHLC data for the given symbol and interval.

        Args:
            symbol: Trading symbol
            exchange: Exchange name (e.g., 'NSE', 'BSE')
            interval: Data interval, e.g. '5minute', 'day'
            duration: Number of past days to fetch

        Returns:
            pd.DataFrame: OHLC data indexed by date, empty if failed
            
        Raises:
            ValueError: If instrument token not found or invalid parameters
        """
        if not self.is_session_active():
            raise ConnectionError("Active session required to fetch OHLC data")
            
        if duration <= 0:
            raise ValueError(f"Duration must be positive, got: {duration}")
            
        instrument_token = self.get_instrument_token(symbol, exchange)
        if instrument_token == -1:
            raise ValueError(f"Instrument token for '{symbol}' not found in exchange '{exchange}'")

        from_date = dt.date.today() - dt.timedelta(days=duration)
        to_date = dt.date.today()

        try:
            logger.info("Fetching OHLC data for %s:%s from %s to %s (%s interval)", 
                       exchange, symbol, from_date, to_date, interval)
            
            raw_data = self.kitesession.historical_data(instrument_token, from_date, to_date, interval)
            
            if not raw_data:
                logger.warning("No OHLC data returned for %s:%s", exchange, symbol)
                return pd.DataFrame()
                
            df = pd.DataFrame(raw_data)
            df.set_index("date", inplace=True)
            
            logger.info("Retrieved %d OHLC records for %s:%s", len(df), exchange, symbol)
            return df
            
        except Exception as e:
            logger.error("Failed to fetch OHLC for %s:%s - %s", exchange, symbol, e)
            return pd.DataFrame()

    def get_instrument_token(self, symbol: str, exchange: str) -> int:
        """
        Get the instrument token for a symbol from cached instrument data.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            
        Returns:
            int: Instrument token, or -1 if not found
            
        Raises:
            ValueError: If instrument data not loaded for exchange
        """
        if exchange not in self.instrument_dfs:
            raise ValueError(f"Instrument data for exchange '{exchange}' not loaded. "
                           f"Call fetch_and_cache_instruments() first.")

        instrument_df = self.instrument_dfs[exchange]
        try:
            matching_instruments = instrument_df[instrument_df.tradingsymbol == symbol]
            if matching_instruments.empty:
                logger.warning("No instrument found for symbol %s on %s", symbol, exchange)
                return -1
                
            token = matching_instruments.instrument_token.iloc[0]
            logger.debug("Found token %d for %s:%s", token, exchange, symbol)
            return int(token)
            
        except Exception as e:
            logger.error("Error fetching token for %s:%s - %s", exchange, symbol, e)
            return -1

    def is_session_active(self) -> bool:
        """Check if the current session is active."""
        try:
            if not self.kitesession or not self.kitesession.access_token:
                return False
            # Try a simple API call to verify session
            self.kitesession.profile()
            return True
        except Exception:
            return False

    def place_intraday_bracket_order(self, tradingsymbol: str, exchange: str, transaction_type: str, 
                                    quantity: int, price: float, squareoff: float, stoploss: float, 
                                    trailing_stoploss: Optional[float] = None) -> Optional[str]:
        """
        Places an intraday Bracket Order (BO) with target and stop loss.

        Args:
            tradingsymbol: Trading symbol of the instrument
            exchange: Exchange name
            transaction_type: "BUY" or "SELL"
            quantity: Quantity to buy/sell
            price: Limit price for the entry order
            squareoff: Target profit in points from entry price
            stoploss: Stop loss in points from entry price
            trailing_stoploss: Optional trailing stop loss in points

        Returns:
            Order ID if placed successfully, None otherwise
            
        Raises:
            ConnectionError: If session is not active
            ValueError: If invalid parameters provided
        """
        if not self.is_session_active():
            raise ConnectionError("Active session required to place bracket orders")
            
        # Validate inputs
        if transaction_type.upper() not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid transaction type: {transaction_type}. Must be 'BUY' or 'SELL'")
            
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got: {quantity}")
            
        if price <= 0:
            raise ValueError(f"Price must be positive, got: {price}")
            
        if squareoff <= 0:
            raise ValueError(f"Squareoff must be positive, got: {squareoff}")
            
        if stoploss <= 0:
            raise ValueError(f"Stoploss must be positive, got: {stoploss}")
            
        try:
            logger.info("Placing bracket order: %s %d %s on %s at %.2f (SL: %.2f, Target: %.2f)", 
                       transaction_type, quantity, tradingsymbol, exchange, price, stoploss, squareoff)
            
            order_id = self.kitesession.place_order(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                transaction_type=getattr(self.kitesession, f"TRANSACTION_TYPE_{transaction_type.upper()}"),
                quantity=quantity,
                order_type=self.kitesession.ORDER_TYPE_LIMIT,
                price=price,
                product=self.kitesession.PRODUCT_MIS,
                variety=self.kitesession.VARIETY_BO,
                squareoff=squareoff,
                stoploss=stoploss,
                trailing_stoploss=trailing_stoploss if trailing_stoploss is not None else 0,
                validity=self.kitesession.VALIDITY_DAY
            )
            
            logger.info("Bracket Order placed successfully. Order ID: %s", order_id)

            # Log order details
            self.order_logger.log_order({
                "order_type": "intraday_bracket",
                "order_id": order_id,
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "price": price,
                "squareoff": squareoff,
                "stoploss": stoploss,
                "trailing_stoploss": trailing_stoploss
            })
            return order_id
            
        except Exception as e:
            logger.error("Failed to place bracket order for %s:%s - %s", exchange, tradingsymbol, e)
            return None

    def place_intraday_order(self, tradingsymbol: str, exchange: str, transaction_type: str, quantity: int) -> Optional[str]:
        """
        Places a market intraday order (MIS product) on the specified exchange.

        Args:
            tradingsymbol: Trading symbol of the instrument, e.g., "RELIANCE"
            exchange: Exchange name, e.g., "NSE"
            transaction_type: Either "BUY" or "SELL"
            quantity: Number of shares or lots to trade

        Returns:
            Order ID if placed successfully, None otherwise
            
        Raises:
            ConnectionError: If session is not active
            ValueError: If invalid parameters provided
        """
        if not self.is_session_active():
            raise ConnectionError("Active session required to place orders")
            
        # Validate inputs
        if transaction_type.upper() not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid transaction type: {transaction_type}. Must be 'BUY' or 'SELL'")
            
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got: {quantity}")
            
        try:
            logger.info("Placing intraday market order: %s %d %s on %s", 
                       transaction_type, quantity, tradingsymbol, exchange)
            
            # Place a market order with intraday margin product (MIS)
            order_id = self.kitesession.place_order(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                transaction_type=getattr(self.kitesession, f"TRANSACTION_TYPE_{transaction_type.upper()}"),
                quantity=quantity,
                order_type=self.kitesession.ORDER_TYPE_MARKET,
                product=self.kitesession.PRODUCT_MIS,
                validity=self.kitesession.VALIDITY_DAY
            )
            
            logger.info("Order placed successfully. Order ID: %s", order_id)

            # Log order details
            self.order_logger.log_order({
                "order_type": "intraday_market",
                "order_id": order_id,
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity
            })
            return order_id
            
        except Exception as e:
            logger.error("Failed to place intraday order for %s:%s - %s", exchange, tradingsymbol, e)
            return None

    def start_live_data_stream(self, instrument_token: int, exchange: str, symbol: str, price_logger) -> KiteTicker:
        """
        Start live data streaming for a given instrument.
        
        Args:
            instrument_token: Instrument token for the symbol
            exchange: Exchange name (e.g., 'NSE', 'BSE')
            symbol: Trading symbol
            price_logger: Price logger instance
            
        Returns:
            KiteTicker instance
            
        Raises:
            ConnectionError: If session is not active
            ValueError: If invalid parameters provided
        """
        if not self.is_session_active():
            raise ConnectionError("Active session required before starting data stream")
        
        if not instrument_token or instrument_token <= 0:
            raise ValueError(f"Invalid instrument token: {instrument_token}")
            
        file_path = self.data_dir / f"ticker_data_{datetime.now().strftime('%Y%m%d')}.csv"
        
        try:
            kws = KiteTicker(API_KEY, self.kitesession.access_token)
            self._active_websockets.append(kws)
            
            with self._managed_csv_file(file_path) as writer:
                def on_ticks(ws, ticks):
                    logger.debug("Received %d ticks", len(ticks))
                    for tick in ticks:
                        if self._validate_tick_data(tick):
                            self._write_full_tick_to_csv(writer, tick)
                            self._update_price_logger(tick, exchange, symbol, price_logger)
                        else:
                            logger.warning("Invalid tick data received: %s", tick)

                def on_connect(ws, response):
                    logger.info("WebSocket connected for %s:%s", exchange, symbol)
                    ws.subscribe([instrument_token])
                    ws.set_mode(ws.MODE_FULL, [instrument_token])

                def on_close(ws, code, reason):
                    logger.info("WebSocket closed for %s:%s - Code: %s, Reason: %s", 
                              exchange, symbol, code, reason)
                    if kws in self._active_websockets:
                        self._active_websockets.remove(kws)

                def on_error(ws, code, reason):
                    logger.error("WebSocket error for %s:%s - Code: %s, Reason: %s", 
                               exchange, symbol, code, reason)

                kws.on_ticks = on_ticks
                kws.on_connect = on_connect
                kws.on_close = on_close
                kws.on_error = on_error

                kws.connect(threaded=True)
                return kws
                
        except Exception as e:
            logger.error("Failed to start live data stream for %s:%s - %s", exchange, symbol, e)
            raise

    # Private methods (alphabetically ordered)
    
    def _initialize_session(self) -> None:
        """Initialize KiteConnect session with proper error handling."""
        try:
            self.kitesession = KiteConnect(api_key=API_KEY)
            logger.info("Login URL: %s", self.kitesession.login_url())
            
            session = self.kitesession.generate_session(
                request_token=REQUEST_TOKEN,
                api_secret=API_SECRET
            )
            self.kitesession.set_access_token(session['access_token'])
            logger.info("KiteConnect session created and access token set successfully.")
            
        except Exception as e:
            logger.error("Failed to create KiteConnect session: %s", e)
            raise ConnectionError(f"Failed to initialize brokerage session: {e}") from e
    
    @contextmanager
    def _managed_csv_file(self, file_path: Path):
        """Context manager for CSV file operations."""
        file_handle = None
        writer = None
        try:
            file_handle = open(file_path, mode='w', newline='', encoding='utf-8')
            writer = csv.writer(file_handle)
            writer.writerow(self.TICKER_CSV_HEADERS)
            yield writer
        except Exception as e:
            logger.error("Error with CSV file %s: %s", file_path, e)
            raise
        finally:
            if file_handle:
                file_handle.close()

    def _update_price_logger(self, tick: Dict[str, Any], exchange: str, symbol: str, price_logger) -> None:
        """Update price logger with tick data, aggregating by minute."""
        last_price = tick.get("last_price")
        timestamp = tick.get("exchange_timestamp")
        
        if last_price is None or timestamp is None:
            logger.debug("Skipping price logger update - missing price or timestamp")
            return
            
        try:
            dt_obj = pd.to_datetime(timestamp)
            dt_minute = dt_obj.replace(second=0, microsecond=0)
            key = (exchange, symbol)
            
            if key not in self._last_logged_tick:
                self._last_logged_tick[key] = (dt_minute, last_price)
                return
                
            last_logged_minute, last_logged_price = self._last_logged_tick[key]
            
            if dt_minute == last_logged_minute:
                # Update price for the same minute
                self._last_logged_tick[key] = (dt_minute, last_price)
            else:
                # New minute - log the previous minute's data
                price_logger.append_live_price(
                    exchange=exchange,
                    symbol=symbol,
                    last_price=last_logged_price,
                    timestamp=last_logged_minute
                )
                self._last_logged_tick[key] = (dt_minute, last_price)
                
        except Exception as e:
            logger.error("Failed to update price logger for %s:%s - %s", exchange, symbol, e)

    def _validate_tick_data(self, tick: Dict[str, Any]) -> bool:
        """Validate tick data before processing."""
        required_fields = ['instrument_token', 'last_price', 'exchange_timestamp']
        return all(field in tick and tick[field] is not None for field in required_fields)

    def _write_full_tick_to_csv(self, writer: csv.writer, tick: Dict[str, Any]) -> None:
        """Write tick data to CSV file."""
        try:
            ohlc = tick.get("ohlc", {})
            writer.writerow([
                tick.get("tradable"),
                tick.get("mode"),
                tick.get("instrument_token"),
                tick.get("last_price"),
                tick.get("last_traded_quantity"),
                tick.get("average_traded_price"),
                tick.get("volume_traded"),
                tick.get("total_buy_quantity"),
                tick.get("total_sell_quantity"),
                ohlc.get("open"),
                ohlc.get("high"),
                ohlc.get("low"),
                ohlc.get("close"),
                tick.get("change"),
                tick.get("last_trade_time"),
                tick.get("oi"),
                tick.get("oi_day_high"),
                tick.get("oi_day_low"),
                tick.get("exchange_timestamp")
            ])
        except Exception as e:
            logger.error("Failed to write tick to CSV: %s", e)

    # Special methods
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except Exception:
            pass  # Ignore errors during cleanup in destructor