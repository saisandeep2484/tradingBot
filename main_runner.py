"""
Main entry point for the TradingBot application.
Handles session setup, instrument fetching, price logging, and ticker streaming.
"""
import logging
import time
from typing import Optional, Any
from kite_session_wrapper import KiteSession
from config.config import EXCHANGES, INTRADAY_TARGET_STOCK
from price_logger import PriceLogger


def setup_logging() -> None:
    """Configure the logging module for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def fetch_and_cache_instruments(kite_session: KiteSession) -> None:
    """Fetch and cache instrument data for all configured exchanges."""
    for exchange in EXCHANGES:
        try:
            kite_session.fetch_and_save_instruments(exchange)
            logging.info(f"Fetched and cached instruments for {exchange}")
        except Exception as e:
            logging.error(f"Failed to fetch instruments for {exchange}: {e}")


def get_instrument_token_from_config(kite_session: KiteSession) -> Optional[tuple]:
    """Parse INTRADAY_TARGET_STOCK from config and return (exchange, symbol, instrument_token)."""
    try:
        exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
        instrument_token = int(kite_session.get_instrument_token(symbol, exchange))
        if instrument_token == -1:
            logging.warning(f"Instrument token not found for {INTRADAY_TARGET_STOCK}")
            return None
        logging.info(f"{INTRADAY_TARGET_STOCK} -> Instrument Token: {instrument_token}")
        return exchange, symbol, instrument_token
    except Exception as e:
        logging.error(f"Error processing {INTRADAY_TARGET_STOCK}: {e}")
        return None


def log_price_data(kite_session: KiteSession, symbol: str, exchange: str) -> None:
    """Log 1-minute price data for previous day and today."""
    try:
        price_logger = PriceLogger(kite_session)
        price_logger.log_previous_day_prices(symbol=symbol, exchange=exchange)
        price_logger.log_today_prices(symbol=symbol, exchange=exchange)
        logging.info(f"Logged price data for {exchange}:{symbol}")
    except Exception as e:
        logging.error(f"Error logging price data for {exchange}:{symbol}: {e}")


def start_ticker_stream(
    kite_session_handler: KiteSession,
    instrument_token: int,
    exchange: str,
    symbol: str,
    price_logger: PriceLogger
) -> Optional[Any]:
    """Start the ticker stream for the given instrument token."""
    try:
        ticker_session = kite_session_handler.start_ticker_stream(
            instrument_token, exchange, symbol, price_logger
        )
        logging.info(f"Ticker session created for token: {instrument_token}")
        return ticker_session
    except Exception as e:
        logging.error(f"Failed to start ticker stream: {e}")
        return None


def main() -> None:
    setup_logging()
    logging.info("Starting TradingBot main runner...")

    kite_session = KiteSession()
    fetch_and_cache_instruments(kite_session)

    token_info = get_instrument_token_from_config(kite_session)
    if not token_info:
        logging.error("No valid instrument token found. Exiting.")
        return
    exchange, symbol, instrument_token = token_info

    log_price_data(kite_session, symbol, exchange)

    price_logger = PriceLogger(kite_session)
    ticker_session = kite_session.start_ticker_stream(
        instrument_token, exchange, symbol, price_logger
    )
    if not ticker_session:
        logging.error("Ticker session could not be created. Exiting.")
        return

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutting down.")


if __name__ == "__main__":
    main()
