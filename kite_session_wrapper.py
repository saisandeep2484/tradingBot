import datetime as dt
import pandas as pd
from datetime import datetime
from order_logger import OrderLogger
from kiteconnect import KiteConnect, KiteTicker
import csv
from config.config import API_KEY, API_SECRET, REQUEST_TOKEN

class KiteSession:
    def __init__(self):
        """
        Handles KiteConnect session creation, instrument utilities, and ticker streaming.
        """
        self.kitesession = KiteConnect(api_key=API_KEY)
        self._last_logged_tick = {}  # {(exchange, symbol): (minute, last_price)}
        self.instrument_dfs = {}  # Cache instrument data per exchange
        self.order_logger = OrderLogger("data_dump/order_log.jsonl")  # Save logs inside data_dump
        self._create_connect_session()

    def _create_connect_session(self):
        try:
            print("Login URL is:", self.kitesession.login_url())
            session = self.kitesession.generate_session(
                request_token=REQUEST_TOKEN,
                api_secret=API_SECRET
            )
            self.kitesession.set_access_token(session['access_token'])  # type: ignore
            print("Connect session created and access token set.")
        except Exception as e:
            print(f"Error creating connect session: {e}")
            raise

    def start_ticker_stream(self, instrument_token, exchange, symbol, price_logger):
        file_path = f"data_dump/ticker_data_{datetime.now().strftime('%Y%m%d')}.csv"
        if not self.kitesession.access_token:
            raise Exception("Connect session must be created before getting ticker session.")

        api_key, access_token = API_KEY, self.kitesession.access_token
        kws = KiteTicker(api_key, access_token)

        # Clear and prepare file
        file = open(file_path, mode='w', newline='')  # Overwrite on every run
        writer = csv.writer(file)

        # Write the CSV header
        writer.writerow([
            "tradable", "mode", "instrument_token", "last_price", "last_traded_quantity",
            "average_traded_price", "volume_traded", "total_buy_quantity", "total_sell_quantity",
            "ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close", "change",
            "last_trade_time", "oi", "oi_day_high", "oi_day_low", "exchange_timestamp"
        ])

        def on_ticks(ws, ticks):
            print("Ticks received:")
            for tick in ticks:
                self._write_full_tick_to_csv(writer, tick)
                file.flush()
                self._update_price_logger(tick, exchange, symbol, price_logger)

        def on_connect(ws, response):
            print("WebSocket connected.")
            ws.subscribe([instrument_token])
            ws.set_mode(ws.MODE_FULL, [instrument_token])

        def on_close(ws, code, reason):
            print("WebSocket closed:", code, reason)
            file.close()

        def on_error(ws, code, reason):
            print("WebSocket error:", code, reason)

        kws.on_ticks = on_ticks  # type: ignore
        kws.on_connect = on_connect  # type: ignore
        kws.on_close = on_close  # type: ignore
        kws.on_error = on_error  # type: ignore

        kws.connect(threaded=True)
        return kws

    def _write_full_tick_to_csv(self, writer, tick):
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
            tick.get("ohlc", {}).get("open"),
            tick.get("ohlc", {}).get("high"),
            tick.get("ohlc", {}).get("low"),
            tick.get("ohlc", {}).get("close"),
            tick.get("change"),
            tick.get("last_trade_time"),
            tick.get("oi"),
            tick.get("oi_day_high"),
            tick.get("oi_day_low"),
            tick.get("exchange_timestamp")
        ])

    def _update_price_logger(self, tick, exchange, symbol, price_logger):
        last_price = tick.get("last_price")
        timestamp = tick.get("exchange_timestamp")
        if last_price is None or timestamp is None:
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
                self._last_logged_tick[key] = (dt_minute, last_price)
            else:
                price_logger.append_live_price(
                    exchange=exchange,
                    symbol=symbol,
                    last_price=last_logged_price,
                    timestamp=last_logged_minute
                )
                self._last_logged_tick[key] = (dt_minute, last_price)
        except Exception as e:
            print(f"Failed to update price logger for {symbol} on {exchange}: {e}")

    def fetch_and_save_instruments(self, exchange):
        try:
            instrument_dump = self.kitesession.instruments(exchange)
            instrument_df = pd.DataFrame(instrument_dump)
            self.instrument_dfs[exchange] = instrument_df

            filename = f"data_dump/{exchange}_Instruments.csv"
            instrument_df.to_csv(filename, index=False, mode='w')

            print(f"Saved {len(instrument_df)} instruments for {exchange} to {filename}")
        except Exception as e:
            print(f"Error fetching instruments for {exchange}: {e}")


    def get_instrument_token(self, symbol, exchange):
        """
        Get the instrument token for a symbol from cached instrument data.
        """
        if exchange not in self.instrument_dfs:
            raise ValueError(f"Instrument data for exchange '{exchange}' not loaded. Call fetch_and_save_instruments() first.")

        instrument_df = self.instrument_dfs[exchange]
        try:
            token = instrument_df[instrument_df.tradingsymbol == symbol].instrument_token.values
            return token[0] if len(token) > 0 else -1
        except Exception as e:
            print(f"Error fetching token for {symbol}: {e}")
            return -1

    def fetch_ohlc(self, symbol, exchange, interval, duration):
        """
        Fetch OHLC data for the given symbol and interval.

        Args:
            symbol (str): Trading symbol.
            exchange (str): Exchange name (e.g., 'NSE', 'BSE').
            interval (str): Data interval, e.g. '5minute', 'day'.
            duration (int): Number of past days to fetch.

        Returns:
            pd.DataFrame: OHLC data indexed by date.
        """
        instrument_token = self.get_instrument_token(symbol, exchange)
        if instrument_token == -1:
            raise ValueError(f"Instrument token for '{symbol}' not found in exchange '{exchange}'")

        from_date = dt.date.today() - dt.timedelta(days=duration)
        to_date = dt.date.today()

        try:
            raw_data = self.kitesession.historical_data(instrument_token, from_date, to_date, interval)
            df = pd.DataFrame(raw_data)
            df.set_index("date", inplace=True)
            return df
        except Exception as e:
            print(f"Failed to fetch OHLC for {symbol}: {e}")
            return pd.DataFrame()

def place_intraday_order(self, tradingsymbol, exchange, transaction_type, quantity):
    """
    Places a market intraday order (MIS product) on the specified exchange.

    Args:
        tradingsymbol (str): Trading symbol of the instrument, e.g., "RELIANCE".
        exchange (str): Exchange name, e.g., "NSE".
        transaction_type (str): Either "BUY" or "SELL".
        quantity (int): Number of shares or lots to trade.

    Returns:
        str or None: Order ID if placed successfully, else None.
    """
    try:
        """
        Place a market order with intraday margin product (MIS).
        - Uses market order type, so executes at the best available price.
        - Valid for the current trading day only.
        """
        order_id = self.kitesession.place_order(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=getattr(self.kitesession, f"TRANSACTION_TYPE_{transaction_type.upper()}"),  # BUY or SELL
            quantity=quantity,
            order_type=self.kitesession.ORDER_TYPE_MARKET,  # Market order
            product=self.kitesession.PRODUCT_MIS,  # Intraday margin product
            validity=self.kitesession.VALIDITY_DAY  # Valid only for the trading day
        )
        print(f"Order placed successfully. Order ID: {order_id}")

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
        print(f"Failed to place order: {e}")
        return None


def place_intraday_bracket_order(self, tradingsymbol, exchange, transaction_type, quantity, price, squareoff, stoploss, trailing_stoploss=None):
    """
    Places an intraday Bracket Order (BO) with target and stop loss.

    Args:
        tradingsymbol (str): Trading symbol of the instrument.
        exchange (str): Exchange name.
        transaction_type (str): "BUY" or "SELL".
        quantity (int): Quantity to buy/sell.
        price (float): Limit price for the entry order.
        squareoff (float): Target profit in points from entry price.
        stoploss (float): Stop loss in points from entry price.
        trailing_stoploss (float, optional): Trailing stop loss in points. Defaults to None.

    Returns:
        str or None: Order ID if placed successfully, else None.
    """
    try:
        """
        Place a limit order with Bracket Order variety:
        - Entry order at specified limit price.
        - Automatically places target (squareoff) and stop loss orders.
        - Optional trailing stop loss adjusts stop loss dynamically.
        - Product is intraday margin (MIS).
        - Order validity is one trading day.
        """
        order_id = self.kitesession.place_order(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=getattr(self.kitesession, f"TRANSACTION_TYPE_{transaction_type.upper()}"),  # BUY or SELL
            quantity=quantity,
            order_type=self.kitesession.ORDER_TYPE_LIMIT,  # Limit order
            price=price,  # Entry price
            product=self.kitesession.PRODUCT_MIS,  # Intraday margin product
            variety=self.kitesession.VARIETY_BO,  # Bracket order type
            squareoff=squareoff,  # Target profit in points
            stoploss=stoploss,  # Stop loss in points
            trailing_stoploss=trailing_stoploss if trailing_stoploss is not None else 0,  # Trailing stop loss points
            validity=self.kitesession.VALIDITY_DAY  # Valid for current trading day
        )
        print(f"Bracket Order placed successfully. Order ID: {order_id}")

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
        print(f"Failed to place bracket order: {e}")
        return None


