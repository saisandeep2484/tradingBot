"""
Main entry point for the TradingBot application.
Handles session setup, instrument fetching, price logging, and ticker streaming.
"""
import logging
import time
from typing import Optional, Any
from core.brokerage_client import BrokerageClient
from config.config import EXCHANGES, INTRADAY_TARGET_STOCK
from data_handlers.price_logger import MarketDataLogger


def setup_logging() -> None:
    """Configure the logging module for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def fetch_and_cache_instruments(brokerage_client: BrokerageClient) -> None:
    """Fetch and cache instrument data for all configured exchanges."""
    for exchange in EXCHANGES:
        try:
            brokerage_client.fetch_and_cache_instruments(exchange)
            logging.info(f"Fetched and cached instruments for {exchange}")
        except Exception as e:
            logging.error(f"Failed to fetch instruments for {exchange}: {e}")


def get_instrument_token_from_config(brokerage_client: BrokerageClient) -> Optional[tuple]:
    """Parse INTRADAY_TARGET_STOCK from config and return (exchange, symbol, instrument_token)."""
    try:
        exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
        instrument_token = int(brokerage_client.get_instrument_token(symbol, exchange))
        if instrument_token == -1:
            logging.warning(f"Instrument token not found for {INTRADAY_TARGET_STOCK}")
            return None
        logging.info(f"{INTRADAY_TARGET_STOCK} -> Instrument Token: {instrument_token}")
        return exchange, symbol, instrument_token
    except Exception as e:
        logging.error(f"Error processing {INTRADAY_TARGET_STOCK}: {e}")
        return None


def log_price_data(brokerage_client: BrokerageClient, symbol: str, exchange: str) -> None:
    """Log 1-minute price data for previous day and today."""
    try:
        price_logger = MarketDataLogger(brokerage_client)
        price_logger.log_previous_day_prices(symbol=symbol, exchange=exchange)
        price_logger.log_today_prices(symbol=symbol, exchange=exchange)
        logging.info(f"Logged price data for {exchange}:{symbol}")
    except Exception as e:
        logging.error(f"Error logging price data for {exchange}:{symbol}: {e}")


def start_ticker_stream(
    brokerage_client: BrokerageClient,
    instrument_token: int,
    exchange: str,
    symbol: str,
    price_logger: MarketDataLogger
) -> Optional[Any]:
    """Start the ticker stream for the given instrument token."""
    try:
        ticker_session = brokerage_client.start_live_data_stream(
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

    brokerage_client = BrokerageClient()
    fetch_and_cache_instruments(brokerage_client)

    token_info = get_instrument_token_from_config(brokerage_client)
    if not token_info:
        logging.error("No valid instrument token found. Exiting.")
        return
    exchange, symbol, instrument_token = token_info

    log_price_data(brokerage_client, symbol, exchange)

    price_logger = MarketDataLogger(brokerage_client)
    ticker_session = brokerage_client.start_live_data_stream(
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
