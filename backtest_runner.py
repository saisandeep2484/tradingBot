import os
import datetime
import pandas as pd

from kite_session_handler import KiteSessionHandler
from kite_session_wrapper import KiteSessionWrapper
from config.config import EXCHANGES, INTRADAY_TARGET_STOCK
from strategies.macd_strategy import MACDStrategy

STOCK_DATA_FOLDER = "stock_data"
INTERVAL = "minute"  # or "day"
DAYS = 60


def ensure_data_folder():
    os.makedirs(STOCK_DATA_FOLDER, exist_ok=True)


def fetch_historical_data(kite_wrapper, symbol, exchange, instrument_token):
    to_date = datetime.datetime.today()
    from_date = to_date - datetime.timedelta(days=DAYS)
    print(f"Fetching historical data for {symbol} ({exchange}) from {from_date.date()} to {to_date.date()}")

    data = kite_wrapper.fetch_ohlc(symbol=symbol, exchange=exchange, interval=INTERVAL, duration=DAYS)
    df = pd.DataFrame(data)

    file_path = os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_historical.csv")
    df.to_csv(file_path, index=False)
    print(f"Saved data to {file_path}")
    return df


def apply_strategies(df, symbol, exchange):
    print(f"\nRunning MACD strategy on {symbol} ({exchange}) with quantity = 100...")
    macd_strategy = MACDStrategy(df, quantity=100)
    trades_df, total_profit = macd_strategy.calculate_profit()
    macd_strategy.plot_signals()

    if trades_df.empty:
        print("No trades were triggered.")
    else:
        print(f"\nCompleted Trades:\n{trades_df}")
        print(f"\nTotal Profit: ₹{total_profit:.2f}")


def load_local_data(symbol, exchange):
    file_path = os.path.join(STOCK_DATA_FOLDER, f"{exchange}_{symbol}_historical.csv")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    try:
        df = pd.read_csv(file_path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        print(f"Failed to load data from {file_path}: {e}")
        return None


def main():
    ensure_data_folder()

    # Toggle between live fetch and CSV
    use_live_data = False

    exchange, symbol = INTRADAY_TARGET_STOCK.split(":")
    
    if use_live_data:
        kite_session_handler = KiteSessionHandler()
        kite = kite_session_handler.create_connect_session()
        wrapper = KiteSessionWrapper(kite)

        try:
            wrapper.fetch_and_save_instruments(exchange)
            instrument_token = int(wrapper.get_instrument_token(symbol, exchange))

            if instrument_token == -1:
                print(f"Instrument token not found for {INTRADAY_TARGET_STOCK}")
                return

            df = fetch_historical_data(wrapper, symbol, exchange, instrument_token)
        except Exception as e:
            print(f"Error fetching data: {e}")
            return
    else:
        df = load_local_data(symbol, exchange)
        if df is None:
            return
        
    df_5min = df.iloc[::5].reset_index(drop=True)
    apply_strategies(df_5min, symbol, exchange)


if __name__ == "__main__":
    main()
