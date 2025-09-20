import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from datetime import datetime  # type: ignore
import os
import time
from typing import Optional, Tuple

class MACDIndicator:
    def __init__(self, 
                 exchange: str,
                 ticker: str,
                 fast_period: int = 12,
                 slow_period: int = 26,
                 signal_period: int = 9):
        """
        Initialize MACD Indicator for real-time calculation

        Args:
            exchange: Exchange name (e.g., 'NSE')
            ticker: Stock ticker (e.g., 'ITI')
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period
        """
        self.exchange = exchange.upper()
        self.ticker = ticker.upper()

        filename = f"{self.exchange}_{self.ticker}_1minute.csv"
        self.input_file = f"stock_data/{filename}"
        self.output_file = f"strategy_data/macd_{filename}"

        os.makedirs("strategy_data", exist_ok=True)

        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

        self.prices = []
        self.timestamps = []
        self.macd_line = []
        self.signal_line = []
        self.histogram = []

        self.fast_ema = None
        self.slow_ema = None
        self.signal_ema = None

        self.last_processed_line = 0

        self.fast_alpha = 2 / (fast_period + 1)
        self.slow_alpha = 2 / (slow_period + 1)
        self.signal_alpha = 2 / (signal_period + 1)

        self._initialize_output_file()

    def _initialize_output_file(self):
        with open(self.output_file, 'w') as f:
            f.write("timestamp,price,macd_line,signal_line,histogram\n")

    def _calculate_ema(self, price: float, prev_ema: Optional[float], alpha: float) -> float:
        return price if prev_ema is None else alpha * price + (1 - alpha) * prev_ema

    def _calculate_sma(self, values: list, period: int) -> float:
        return sum(values[-period:]) / period

    def _read_new_data(self) -> list:
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

    def _process_new_price(self, timestamp: datetime, price: float):
        self.timestamps.append(timestamp)
        self.prices.append(price)

        if len(self.prices) == 1:
            self.fast_ema = price
            self.slow_ema = price
            macd_value = 0
            signal_value = 0
        elif len(self.prices) <= self.slow_period:
            self.fast_ema = self._calculate_sma(self.prices, self.fast_period) if len(self.prices) >= self.fast_period else self._calculate_ema(price, self.fast_ema, self.fast_alpha)
            self.slow_ema = self._calculate_sma(self.prices, self.slow_period) if len(self.prices) == self.slow_period else self._calculate_ema(price, self.slow_ema, self.slow_alpha)
            macd_value = self.fast_ema - self.slow_ema
            signal_value = self.signal_ema if self.signal_ema is not None else macd_value
        else:
            self.fast_ema = self._calculate_ema(price, self.fast_ema, self.fast_alpha)
            self.slow_ema = self._calculate_ema(price, self.slow_ema, self.slow_alpha)
            macd_value = self.fast_ema - self.slow_ema

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

        self.macd_line.append(macd_value)
        self.signal_line.append(signal_value)
        self.histogram.append(histogram_value)

        return macd_value, signal_value, histogram_value

    def _write_to_output(self, timestamp: datetime, price: float, macd_value: float, signal_value: float, histogram_value: float):
        with open(self.output_file, 'a') as f:
            f.write(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')},{price:.2f},{macd_value:.4f},{signal_value:.4f},{histogram_value:.4f}\n")

    def update(self):
        new_data = self._read_new_data()

        for timestamp, price in new_data:
            macd_value, signal_value, histogram_value = self._process_new_price(timestamp, price)
            self._write_to_output(timestamp, price, macd_value, signal_value, histogram_value)

            print(f"{timestamp}: Price={price:.2f}, MACD={macd_value:.4f}, Signal={signal_value:.4f}, Histogram={histogram_value:.4f}")

        return len(new_data)

    def get_latest_values(self) -> Optional[Tuple[float, float, float]]:
        if not self.macd_line:
            return None
        return (self.macd_line[-1], self.signal_line[-1], self.histogram[-1])

    def start_streaming(self, interval: float = 1.0):
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
        print("Processing existing data...")
        count = self.update()
        print(f"Processed {count} records")

        if count > 0:
            latest = self.get_latest_values()
            if latest:
                print(f"Latest MACD values - MACD: {latest[0]:.4f}, Signal: {latest[1]:.4f}, Histogram: {latest[2]:.4f}")

