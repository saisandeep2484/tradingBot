import pandas as pd
import matplotlib.pyplot as plt

class MACDStrategy:
    def __init__(self, df, quantity=100):  # default to 100 shares
        self.df = df.copy()
        self.quantity = quantity

    def generate_signals(self):
        self.df["EMA_12"] = self.df["close"].ewm(span=12, adjust=False).mean()
        self.df["EMA_26"] = self.df["close"].ewm(span=26, adjust=False).mean()
        self.df["MACD"] = self.df["EMA_12"] - self.df["EMA_26"]
        self.df["Signal"] = self.df["MACD"].ewm(span=9, adjust=False).mean()

        self.df["Buy_Signal"] = (self.df["MACD"] > self.df["Signal"]) & (self.df["MACD"].shift(1) <= self.df["Signal"].shift(1))
        self.df["Sell_Signal"] = (self.df["MACD"] < self.df["Signal"]) & (self.df["MACD"].shift(1) >= self.df["Signal"].shift(1))

        return self.df
    
    def plot_signals(self):
        self.generate_signals()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

        # Price and buy/sell markers
        ax1.plot(self.df["close"], label="Close Price", color='gray')
        ax1.scatter(self.df.index[self.df["Buy_Signal"]], self.df["close"][self.df["Buy_Signal"]],
                    label="Buy", marker="^", color="green", s=100)
        ax1.scatter(self.df.index[self.df["Sell_Signal"]], self.df["close"][self.df["Sell_Signal"]],
                    label="Sell", marker="v", color="red", s=100)
        ax1.set_title("Price with Buy/Sell Signals")
        ax1.legend()
        ax1.grid(True)

        # MACD and Signal line
        ax2.plot(self.df["MACD"], label="MACD", color='blue')
        ax2.plot(self.df["Signal"], label="Signal Line", color='orange')
        ax2.bar(self.df.index, self.df["MACD"] - self.df["Signal"], label="MACD Histogram",
                color=(self.df["MACD"] - self.df["Signal"]).apply(lambda x: 'green' if x > 0 else 'red'))
        ax2.set_title("MACD Indicator")
        ax2.legend()
        ax2.grid(True)

        plt.tight_layout()
        plt.show()

    def calculate_profit(self):
        signals = self.generate_signals()
        trades = []
        position = None

        for index, row in signals.iterrows():
            if row["Buy_Signal"] and position is None:
                position = {
                    "buy_price": row["close"]
                }
            elif row["Sell_Signal"] and position is not None:
                sell_price = row["close"]
                profit = (sell_price - position["buy_price"]) * self.quantity
                trades.append({
                    "buy_price": position["buy_price"],
                    "sell_price": sell_price,
                    "profit": profit
                })
                position = None

        trades_df = pd.DataFrame(trades)
        total_profit = trades_df["profit"].sum() if not trades_df.empty else 0.0

        return trades_df, total_profit
